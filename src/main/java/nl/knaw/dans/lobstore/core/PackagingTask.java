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
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import java.io.IOException;
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
    private final TransferRequestDao transferRequestDao;
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
            Path bucketFolder = uploadDir.resolve(bucketId.toString());

            if (!Files.exists(bucketFolder)) {
                Files.createDirectories(bucketFolder);
            }

            for (var tr : bucket.getTransferRequests()) {
                Path sourceFile = downloadDir.resolve(tr.getId().toString()).resolve(tr.getSha1Sum());
                Path targetFile = bucketFolder.resolve(tr.getId().toString());

                if (!Files.exists(targetFile)) {
                    log.debug("Moving file from {} to {}", sourceFile, targetFile);
                    Files.move(sourceFile, targetFile);
                } else {
                    log.debug("Target file already exists: {}", targetFile);
                }

                // Release base-claim for each of those files. Target was "download" in DownloadTaskSource.
                quotaManager.release(tr.getId() + "/base", TARGET_DOWNLOAD);
            }

            // Call packaging command with path to bucket
            executePackagingCommand(bucketFolder);

            bucket.setStatus(BucketStatus.PACKAGED);
            bucketDao.save(bucket);
            
            // Release both /base and /extra claims on the upload folder
            quotaManager.release(bucketId + "/base", TARGET_UPLOAD);
            quotaManager.release(bucketId + "/extra", TARGET_UPLOAD);
            
            log.info("Successfully finished PACKAGING task for bucket {}", bucketId);
        }
        catch (IOException e) {
            log.error("IO error during packaging for bucket {}", bucketId, e);
            // Re-throw to allow for retries or manual intervention if it's considered recoverable
            // The prompt says: "If it fails because of a recoverable problem it leaves the state as PACKAGING, otherwise it changes it to FAILED."
            // For now, let's assume IO errors might be transient, so we don't change state to FAILED.
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

    private void executePackagingCommand(Path bucketFolder) throws IOException, InterruptedException {
        log.debug("Executing packaging command: {} {}", packagingCommand, bucketFolder);
        ProcessBuilder pb = new ProcessBuilder(packagingCommand, bucketFolder.toAbsolutePath().toString());
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        
        if (exitCode != 0) {
            // Check if it's a recoverable problem.
            // For now we assume non-zero is non-recoverable unless we have more info.
            throw new RuntimeException("Packaging command failed with exit code " + exitCode);
        }
    }
}
