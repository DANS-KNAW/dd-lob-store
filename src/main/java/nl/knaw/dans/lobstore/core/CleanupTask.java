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
import nl.knaw.dans.lobstore.db.ClaimDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class CleanupTask implements Runnable {
    private static final Set<PosixFilePermission> deletePermissions = PosixFilePermissions.fromString("rwxr-xr-x");

    private final UUID bucketId;
    private final BucketDao bucketDao;
    private final TransferRequestDao transferRequestDao;
    private final ClaimDao claimDao;
    private final Path uploadDir;
    private final Path downloadDir;

    @Override
    @UnitOfWork
    public void run() {
        try {
            log.info("Starting CLEANUP task for bucket {}", bucketId);
            Bucket bucket = bucketDao.findById(bucketId).orElse(null);
            
            if (bucket == null) {
                log.info("Bucket {} already removed.", bucketId);
                return;
            }

            // Remove download directories and TransferRequest claims/records
            for (var tr : bucket.getTransferRequests()) {
                Path trDir = downloadDir.resolve(tr.getId().toString());
                if (Files.exists(trDir)) {
                    log.debug("Removing transfer request directory: {}", trDir);
                    FileUtils.deleteDirectory(trDir.toFile());
                }
                
                claimDao.deleteByIdStartingWith(tr.getId().toString());
                transferRequestDao.delete(tr);
                log.debug("Removed transfer request {}", tr.getId());
            }

            // Ensure bucket directories are removed
            Path bucketFile = uploadDir.resolve(bucketId.toString() + ".dmftar");
            if (Files.exists(bucketFile)) {
                try (var stream = Files.walk(bucketFile)) {
                    stream.filter(Files::isDirectory).forEach(path -> {
                        try {
                            Files.setPosixFilePermissions(path, deletePermissions);
                        }
catch (IOException | UnsupportedOperationException e) {
                            log.warn("Failed to set delete permissions for {}: {}", path, e.getMessage());
                        }
                    });
                }
                log.debug("Removing local bucket file: {}", bucketFile);
                FileUtils.deleteDirectory(bucketFile.toFile());
            }

            Path bucketFolder = uploadDir.resolve(bucketId.toString());
            if (Files.exists(bucketFolder)) {
                log.debug("Removing local bucket folder: {}", bucketFolder);
                FileUtils.deleteDirectory(bucketFolder.toFile());
            }

            // Remove bucket claims and bucket record itself
            claimDao.deleteByIdStartingWith(bucketId.toString());
            bucketDao.delete(bucket);

            log.info("Successfully finished CLEANUP task for bucket {}", bucketId);
        }
        catch (Exception e) {
            log.error("Error during cleanup for bucket {}", bucketId, e);
        }
    }
}
