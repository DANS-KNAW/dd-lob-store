/*
 * Copyright (C) 2026 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.lobstore.core;

import io.dropwizard.hibernate.UnitOfWork;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.BasicFileAccessApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.GetFileRange;
import nl.knaw.dans.lobstore.config.DownloadConfig;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.hibernate.HibernateException;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class DownloadTask implements Runnable {
    private final UUID transferRequestId;
    private final TransferRequestDao transferRequestDao;
    private final DataverseClient dataverseClient;
    private final DownloadConfig downloadConfig;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final ExecutorService chunkDownloadExecutor;

    @Override
    @UnitOfWork
    public void run() {
        log.info("Starting DOWNLOAD step for {}", transferRequestId);
        try {
            TransferRequest transferRequest = transferRequestDao.findById(transferRequestId)
                .orElseThrow(() -> new RuntimeException("Transfer request with id " + transferRequestId + " not found"));

            Path downloadDir = downloadConfig.getDownloadDirectory().resolve(transferRequest.getId().toString());
            if (Files.exists(downloadDir)) {
                log.info("Download directory {} already exists, resuming download...", downloadDir);
            }
            else {
                Files.createDirectories(downloadDir);
            }

            BasicFileAccessApi basicFileAccessApi = dataverseClient.basicFileAccess(transferRequest.getDataverseFileId());
            long fileSize = transferRequest.getFileSize();
            long chunkSize = downloadConfig.getChunkSize().toBytes();
            String sha1 = transferRequest.getSha1Sum();
            Path outputFile = downloadDir.resolve(sha1);

            if (fileSize < chunkSize) {
                downloadWholeFile(basicFileAccessApi, outputFile);
            }
            else {
                downloadInChunks(basicFileAccessApi, downloadDir, fileSize, chunkSize, sha1);
                mergeChunks(downloadDir, sha1, fileSize, chunkSize);
            }

            verifySha1(outputFile, sha1);
            deleteChunks(downloadDir, sha1);

            transferRequest.setStatus(TransferRequestStatus.DOWNLOADED);
            transferRequestDao.save(transferRequest);
            quotaManager.release(transferRequest.getId() + "/extra", "download");
            log.info("Finished DOWNLOAD step for {}", transferRequestId);
        }
        catch (DataverseException e) {
            if (isRecoverable(e.getStatus())) {
                log.warn("Recoverable Dataverse error ({}) for {}: {}, will retry", e.getStatus(), transferRequestId, e.getMessage());
            }
            else {
                log.error("Permanent Dataverse error ({}) for {}: {}", e.getStatus(), transferRequestId, e.getMessage());
                handleFailure(e);
            }
        }
        catch (IOException | HibernateException e) {
            log.warn("Transient error occurred during download for {}: {}, will retry later", transferRequestId, e.getMessage());
        }
        catch (Exception e) {
            if (isInterrupted(e)) {
                log.warn("Download task for {} was interrupted", transferRequestId);
                Thread.currentThread().interrupt();
            }
            else {
                log.error("Error downloading file for transfer request with id {}", transferRequestId, e);
                handleFailure(e);
                // Do not rethrow to avoid the database transaction from being rolled back.
            }
        }
        finally {
            activeTaskRegistry.remove(transferRequestId);
        }
    }

    private boolean isInterrupted(Throwable e) {
        if (e instanceof InterruptedException) {
            return true;
        }
        if (e.getCause() != null && e.getCause() != e) {
            return isInterrupted(e.getCause());
        }
        return false;
    }

    private boolean isRecoverable(int status) {
        return status == 429 || status == 502 || status == 503 || status == 504;
    }

    private void handleFailure(Throwable e) {
        transferRequestDao.findById(transferRequestId).ifPresent(transferRequest -> {
            transferRequest.setStatus(TransferRequestStatus.FAILED);
            String msg = e.getMessage();
            if (e.getCause() != null) {
                msg += ": " + e.getCause().getMessage();
            }
            transferRequest.setMessage("Error downloading file: " + msg);
            transferRequestDao.save(transferRequest);
        });
    }

    private void downloadWholeFile(BasicFileAccessApi api, Path outputFile) throws IOException, DataverseException {
        if (Files.exists(outputFile)) {
            log.info("File {} already exists, skipping download", outputFile);
            return;
        }
        log.info("Downloading whole file to {}", outputFile);
        api.getFile(response -> {
            try (InputStream is = response.getEntity().getContent()) {
                FileUtils.copyInputStreamToFile(is, outputFile.toFile());
            }
            return null;
        });
    }

    private void downloadInChunks(BasicFileAccessApi api, Path downloadDir, long fileSize, long chunkSize, String sha1) throws InterruptedException {
        int maxChunksPerFile = downloadConfig.getMaxChunksPerFile();
        Semaphore semaphore = new Semaphore(maxChunksPerFile);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicReference<Throwable> firstError = new AtomicReference<>();

        for (long start = 0; start < fileSize; start += chunkSize) {
            if (firstError.get() != null) {
                break;
            }

            long end = Math.min(start + chunkSize, fileSize);
            int chunkIndex = (int) (start / chunkSize);
            Path chunkFile = downloadDir.resolve(sha1 + "." + chunkIndex);
            Path chunkFilePartial = chunkFile.resolveSibling(chunkFile.getFileName() + ".part");

            if (Files.exists(chunkFile)) {
                log.debug("Chunk {} already exists, skipping", chunkFile);
                continue;
            }

            try {
                Files.deleteIfExists(chunkFilePartial);
            }
            catch (IOException e) {
                log.warn("Could not delete partial chunk file {}", chunkFilePartial, e);
            }
            GetFileRange range = new GetFileRange(start, end - 1);
            semaphore.acquire();
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    if (firstError.get() != null) {
                        return;
                    }
                    log.debug("Downloading chunk {} ({}-{}) for {}", chunkIndex, range.getStart(), range.getEnd(), transferRequestId);
                    api.getFile(range, response -> {
                        try (InputStream is = response.getEntity().getContent()) {
                            FileUtils.copyInputStreamToFile(is, chunkFilePartial.toFile());
                            Files.move(chunkFilePartial, chunkFile);
                        }
                        return null;
                    });
                }
                catch (Exception e) {
                    log.error("Error downloading chunk {} for {}", chunkIndex, transferRequestId, e);
                    firstError.set(e);
                }
                finally {
                    semaphore.release();
                }
            }, chunkDownloadExecutor);
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).handle((v, e) -> null).join();

        if (firstError.get() != null) {
            throw new RuntimeException("One or more chunks failed to download", firstError.get());
        }
    }

    private void deleteChunks(Path downloadDir, String sha1) {
        log.info("Deleting chunks in {}", downloadDir);
        try (Stream<Path> files = Files.list(downloadDir)) {
            files.filter(path -> {
                String filename = path.getFileName().toString();
                return filename.startsWith(sha1 + ".");
            }).forEach(path -> {
                try {
                    Files.delete(path);
                }
                catch (IOException e) {
                    log.warn("Could not delete chunk file {}", path, e);
                }
            });
        }
        catch (IOException e) {
            log.error("Error listing files in {}", downloadDir, e);
        }
    }

    private void mergeChunks(Path downloadDir, String sha1, long fileSize, long chunkSize) throws IOException {
        Path outputFile = downloadDir.resolve(sha1);
        if (Files.exists(outputFile)) {
            log.info("Final file {} already exists, skipping merge", outputFile);
            return;
        }

        log.info("Merging chunks into {}", outputFile);
        try (OutputStream out = new FileOutputStream(outputFile.toFile())) {
            for (long start = 0; start < fileSize; start += chunkSize) {
                int chunkIndex = (int) (start / chunkSize);
                Path chunkFile = downloadDir.resolve(sha1 + "." + chunkIndex);
                if (!Files.exists(chunkFile)) {
                    throw new IOException("Missing chunk file: " + chunkFile);
                }
                Files.copy(chunkFile, out);
            }
        }
    }

    private void verifySha1(Path file, String expectedSha1) throws IOException {
        log.info("Verifying SHA-1 for {}", file);
        try (InputStream is = new BufferedInputStream(new FileInputStream(file.toFile()))) {
            String actualSha1 = DigestUtils.sha1Hex(is);
            if (!actualSha1.equalsIgnoreCase(expectedSha1)) {
                throw new RuntimeException(String.format("SHA-1 mismatch for %s. Expected: %s, Actual: %s", file, expectedSha1, actualSha1));
            }
        }
    }
}
