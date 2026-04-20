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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskSource;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class PackagingTaskSource implements TaskSource<Bucket> {
    // Packaging is done in the upload directory
    private static final String TARGET_UPLOAD = "upload";

    private final TransferRequestDao transferRequestDao;
    private final BucketDao bucketDao;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final long minimalBucketSize;
    private final long margin;

    @Override
    public List<Bucket> nextInputs() {
        // 1. Check for interrupted buckets
        var interruptedBuckets = bucketDao.findByStatus(BucketStatus.PACKAGING);
        for (var bucket : interruptedBuckets) {
            if (!activeTaskRegistry.contains(bucket.getId())) {
                log.info("Restarting interrupted packaging task for bucket {}", bucket.getId());
                activeTaskRegistry.add(bucket.getId());
                return List.of(bucket);
            }
        }

        // 2. Existing logic for creating new buckets
        var packagableItems = transferRequestDao.findPackagableItems();
        if (packagableItems.isEmpty()) {
            return List.of();
        }

        long currentTotalSize = 0;
        List<TransferRequest> itemsToPackage = new ArrayList<>();
        
        for (var item : packagableItems) {
            itemsToPackage.add(item);
            currentTotalSize += item.getFileSize();
            if (currentTotalSize >= minimalBucketSize) {
                break;
            }
        }

        if (currentTotalSize < minimalBucketSize) {
            log.debug("Total size of packagable items ({}) is less than minimal bucket size ({})", currentTotalSize, minimalBucketSize);
            return List.of();
        }

        UUID bucketId = UUID.randomUUID();
        // Claim both /base and /extra on the upload folder.
        if (quotaManager.ensureClaimed(bucketId + "/base", TARGET_UPLOAD, currentTotalSize) &&
            quotaManager.ensureClaimed(bucketId + "/extra", TARGET_UPLOAD, currentTotalSize + margin)) {
            Bucket bucket = Bucket.builder()
                .id(bucketId)
                .status(BucketStatus.PACKAGING)
                .build();
            bucketDao.save(bucket);
            
            for (var item : itemsToPackage) {
                item.setBucket(bucket);
                transferRequestDao.save(item);
            }
            
            // Ensure the bucket object has the transfer requests populated if needed later, 
            // though PackagingTask fetches it from DB anyway.
            bucket.setTransferRequests(itemsToPackage);

            activeTaskRegistry.add(bucketId);
            return List.of(bucket);
        }

        return List.of();
    }
}
