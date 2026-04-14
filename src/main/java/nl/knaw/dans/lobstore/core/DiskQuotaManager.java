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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lobstore.db.DiskClaimDao;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@AllArgsConstructor
public class DiskQuotaManager {
    private final DiskClaimDao diskClaimDao;
    private final Path downloadFolder;
    private final Path uploadFolder;
    private final long downloadQuotaBytes;
    private final long uploadQuotaBytes;
    private final boolean disableQuotaEnforcement;

    public synchronized boolean claim(UUID jobId, String folder, long amountBytes) throws IOException {
        if (disableQuotaEnforcement) {
            return true;
        }

        long quota = folder.equals("download") ? downloadQuotaBytes : uploadQuotaBytes;
        Path path = folder.equals("download") ? downloadFolder : uploadFolder;

        long claimed = diskClaimDao.findByFolder(folder).stream()
                .mapToLong(DiskClaim::getAmount)
                .sum();

        FileStore store = Files.getFileStore(path);
        long usableSpace = store.getUsableSpace();

        if (claimed + amountBytes > quota || amountBytes > usableSpace) {
            log.warn("Cannot claim {} bytes in {}. Claimed: {}, Quota: {}, Usable: {}", amountBytes, folder, claimed, quota, usableSpace);
            return false;
        }

        diskClaimDao.create(DiskClaim.builder()
                .id(UUID.randomUUID())
                .jobId(jobId)
                .folder(folder)
                .amount(amountBytes)
                .creationTimestamp(OffsetDateTime.now())
                .build());

        return true;
    }

    public synchronized void release(UUID jobId) {
        diskClaimDao.deleteByJobId(jobId);
    }

    public synchronized void release(UUID jobId, String folder) {
        diskClaimDao.deleteByJobIdAndFolder(jobId, folder);
    }
}
