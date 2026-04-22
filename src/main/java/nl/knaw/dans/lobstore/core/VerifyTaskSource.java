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

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class VerifyTaskSource implements TaskSource<Bucket> {

    private final BucketDao bucketDao;
    private final ActiveTaskRegistry activeTaskRegistry;

    @Override
    public Optional<Bucket> nextInput() {
        // 1. Check for interrupted buckets in VERIFYING state
        var interruptedBuckets = bucketDao.findByStatus(BucketStatus.VERIFYING);
        for (var bucket : interruptedBuckets) {
            if (!activeTaskRegistry.contains(bucket.getId())) {
                log.info("Restarting interrupted verify task for bucket {}", bucket.getId());
                activeTaskRegistry.add(bucket.getId());
                return Optional.of(bucket);
            }
        }

        // 2. Check for buckets in UPLOADED state (ready for first-time verification)
        var uploadedBuckets = bucketDao.findByStatus(BucketStatus.UPLOADED);
        for (var bucket : uploadedBuckets) {
            if (!activeTaskRegistry.contains(bucket.getId())) {
                log.info("Starting verify task for bucket {}", bucket.getId());
                bucket.setStatus(BucketStatus.VERIFYING);
                bucketDao.save(bucket);
                activeTaskRegistry.add(bucket.getId());
                return Optional.of(bucket);
            }
        }

        return Optional.empty();
    }
}
