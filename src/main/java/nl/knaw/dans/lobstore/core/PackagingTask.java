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

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.DefaultExecutor.Builder;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import io.dropwizard.hibernate.UnitOfWork;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class PackagingTask implements Runnable {
    private static final String TARGET_DOWNLOAD = "download";
    private static final String TARGET_UPLOAD = "upload";

    private final UUID bucketId;
    private final BucketDao bucketDao;
    private final Path downloadDir;
    private final Path uploadDir;
    private final String packagingCommand;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;

    @Override
    @UnitOfWork
    public void run() {
        try {
            log.info("Starting PACKAGING task for bucket {}", bucketId);
            Bucket bucket = bucketDao.findById(bucketId).orElseThrow(() -> new RuntimeException("Bucket not found: " + bucketId));

            // Sanity check: all transfer requests must have the same datastation
            String datastation = bucket.getDatastation();
            for (var tr : bucket.getTransferRequests()) {
                if (!tr.getDatastation().equals(datastation)) {
                    log.error("Sanity check failed: TransferRequest {} has datastation {}, but bucket {} has datastation {}",
                        tr.getId(), tr.getDatastation(), bucketId, datastation);
                    throw new IllegalStateException("Mixed datastations in bucket " + bucketId);
                }
            }

            Path bucketFolder = uploadDir.resolve(bucketId.toString());

            if (!Files.exists(bucketFolder)) {
                Files.createDirectories(bucketFolder);
            }

            for (var tr : bucket.getTransferRequests()) {
                Path sourceFile = downloadDir.resolve(tr.getId().toString()).resolve(tr.getSha1Sum());
                Path targetFile = bucketFolder.resolve(tr.getSha1Sum());

                if (!Files.exists(targetFile)) {
                    log.debug("Moving file from {} to {}", sourceFile, targetFile);
                    Files.move(sourceFile, targetFile);
                    Path parentDir = sourceFile.getParent();
                    try {
                        Files.delete(parentDir);
                        log.debug("Deleted empty download directory: {}", parentDir);
                    } catch (DirectoryNotEmptyException e) {
                        log.warn("Download directory is not empty and was not deleted: {}", parentDir);
                    } catch (IOException e) {
                        log.error("Failed to delete download directory: {}", parentDir, e);
                    }
                } else {
                    log.debug("Target file already exists: {}", targetFile);
                }

                // Release base-claim for each of those files. Target was "download" in DownloadTaskSource.
                quotaManager.release(tr.getId() + "/base", TARGET_DOWNLOAD);
            }

            // Replace ${bucketname} and handle partial output deletion
            String bucketName = bucketId.toString();
            String command = packagingCommand.replace("${bucketname}", bucketName);
            Path outputFile = uploadDir.resolve(bucketName + ".tar");

            if (Files.exists(outputFile)) {
                log.info("Deleting partial output file from previous attempt: {}", outputFile);
                Files.delete(outputFile);
            }

            // Call packaging command
            executePackagingCommand(command);

            bucket.setStatus(BucketStatus.PACKAGED);
            bucketDao.save(bucket);

            FileUtils.deleteDirectory(bucketFolder.toFile());
            quotaManager.release(bucketId + "/extra", TARGET_UPLOAD);
            
            log.info("Successfully finished PACKAGING task for bucket {}", bucketId);
        }
        catch (IOException e) {
            log.error("IO error during packaging for bucket {}", bucketId, e);
            // Re-throw to allow for retries or manual intervention if it's considered recoverable
        }
        catch (Exception e) {
            log.error("Error during packaging for bucket {}", bucketId, e);
            bucketDao.findById(bucketId).ifPresent(bucket -> {
                bucket.setStatus(BucketStatus.FAILED);
                bucketDao.save(bucket);
            });
        }
        finally {
            activeTaskRegistry.remove(bucketId);
        }
    }

    private void executePackagingCommand(String command) throws IOException {
        log.debug("Executing packaging command: {}", command);
        CommandLine commandLine = new CommandLine("sh");
        commandLine.addArgument("-c");
        commandLine.addArgument(command, false);

        DefaultExecutor executor = new Builder<>().get();
        executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));
        
        try {
            int exitCode = executor.execute(commandLine);
            if (exitCode != 0) {
                throw new RuntimeException("Packaging command failed with exit code " + exitCode);
            }
        } catch (ExecuteException e) {
            throw new RuntimeException("Packaging command failed", e);
        }
    }
}
