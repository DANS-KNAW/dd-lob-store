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

import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PackagingTaskSourceTest {

    private final TransferRequestDao transferRequestDao = mock(TransferRequestDao.class);
    private final BucketDao bucketDao = mock(BucketDao.class);
    private final QuotaManager quotaManager = mock(QuotaManager.class);
    private final ActiveTaskRegistry activeTaskRegistry = new ActiveTaskRegistry();

    @Test
    void nextInputs_should_prioritize_interrupted_buckets() {
        UUID interruptedBucketId = UUID.randomUUID();
        TransferRequest tr = TransferRequest.builder().id(UUID.randomUUID()).build();
        Bucket interruptedBucket = Bucket.builder()
            .id(interruptedBucketId)
            .status(BucketStatus.PACKAGING)
            .transferRequests(List.of(tr))
            .build();

        when(bucketDao.findByStatus(BucketStatus.PACKAGING)).thenReturn(List.of(interruptedBucket));

        PackagingTaskSource source = new PackagingTaskSource(transferRequestDao, bucketDao, quotaManager, activeTaskRegistry, 1000, 100);
        
        List<Bucket> result = source.nextInputs();
        
        assertThat(result).containsExactly(interruptedBucket);
        assertThat(activeTaskRegistry.contains(interruptedBucketId)).isTrue();
    }

    @Test
    void nextInputs_should_not_pick_up_already_active_interrupted_buckets() {
        UUID activeBucketId = UUID.randomUUID();
        activeTaskRegistry.add(activeBucketId);
        
        TransferRequest tr = TransferRequest.builder().id(UUID.randomUUID()).build();
        Bucket activeBucket = Bucket.builder()
            .id(activeBucketId)
            .status(BucketStatus.PACKAGING)
            .transferRequests(List.of(tr))
            .build();

        when(bucketDao.findByStatus(BucketStatus.PACKAGING)).thenReturn(List.of(activeBucket));
        when(transferRequestDao.findPackagableItems()).thenReturn(List.of());

        PackagingTaskSource source = new PackagingTaskSource(transferRequestDao, bucketDao, quotaManager, activeTaskRegistry, 1000, 100);
        
        List<Bucket> result = source.nextInputs();
        
        assertThat(result).isEmpty();
    }
}
