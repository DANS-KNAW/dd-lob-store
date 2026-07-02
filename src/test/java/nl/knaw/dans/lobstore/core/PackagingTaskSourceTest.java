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

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PackagingTaskSourceTest {

    private final TransferRequestDao transferRequestDao = mock(TransferRequestDao.class);
    private final BucketDao bucketDao = mock(BucketDao.class);
    private final QuotaManager quotaManager = mock(QuotaManager.class);
    private final ActiveTaskRegistry activeTaskRegistry = new ActiveTaskRegistry();

    @Test
    void nextInput_should_prioritize_interrupted_buckets() {
        UUID interruptedBucketId = UUID.randomUUID();
        TransferRequest tr = TransferRequest.builder().id(UUID.randomUUID()).build();
        Bucket interruptedBucket = Bucket.builder()
            .id(interruptedBucketId)
            .status(BucketStatus.PACKAGING)
            .transferRequests(List.of(tr))
            .build();

        when(bucketDao.findByStatus(BucketStatus.PACKAGING)).thenReturn(List.of(interruptedBucket));

        PackagingTaskSource source = new PackagingTaskSource(transferRequestDao, bucketDao, quotaManager, activeTaskRegistry, 1000, 100);

        Optional<Bucket> result = source.nextInput();

        assertThat(result).contains(interruptedBucket);
        assertThat(activeTaskRegistry.contains(interruptedBucketId)).isTrue();
    }

    @Test
    void nextInput_should_not_pick_up_already_active_interrupted_buckets() {
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

        Optional<Bucket> result = source.nextInput();

        assertThat(result).isEmpty();
    }

    @Test
    void nextInput_should_release_base_claim_when_extra_claim_fails() {
        String datastation = "ds1";
        TransferRequest tr = TransferRequest.builder().id(UUID.randomUUID()).fileSize(100L).datastation(datastation).build();

        when(bucketDao.findByStatus(BucketStatus.PACKAGING)).thenReturn(List.of());
        when(transferRequestDao.findDatastationsReadyForPackaging(anyLong())).thenReturn(List.of(datastation));
        when(transferRequestDao.findPackagableItemsByDatastation(datastation)).thenReturn(List.of(tr));
        // /base succeeds, /extra fails
        when(quotaManager.ensureClaimed(anyString(), eq("upload"), anyLong()))
            .thenAnswer(inv -> inv.getArgument(0, String.class).endsWith("/base"));

        PackagingTaskSource source = new PackagingTaskSource(transferRequestDao, bucketDao, quotaManager, activeTaskRegistry, 50L, 10L);

        var result = source.nextInput();

        assertThat(result).isEmpty();
        // The /base claim must be released so it does not accumulate across poll cycles.
        verify(quotaManager).release(anyString(), eq("upload"));
        // No bucket must have been persisted.
        verify(bucketDao, never()).save(any());
    }

    @Test
    void nextInput_should_limit_bucket_size_and_include_exceeding_item() {
        String datastation = "ds1";
        TransferRequest tr1 = TransferRequest.builder().id(UUID.randomUUID()).fileSize(99L).datastation(datastation).build();
        TransferRequest tr2 = TransferRequest.builder().id(UUID.randomUUID()).fileSize(150L).datastation(datastation).build();
        TransferRequest tr3 = TransferRequest.builder().id(UUID.randomUUID()).fileSize(50L).datastation(datastation).build();

        when(bucketDao.findByStatus(BucketStatus.PACKAGING)).thenReturn(List.of());
        when(transferRequestDao.findDatastationsReadyForPackaging(anyLong())).thenReturn(List.of(datastation));
        when(transferRequestDao.findPackagableItemsByDatastation(datastation)).thenReturn(List.of(tr1, tr2, tr3));

        // Threshold is 200. tr1(100) + tr2(150) = 250, so tr2 exceeds the threshold and should be the last item.
        PackagingTaskSource source = new PackagingTaskSource(transferRequestDao, bucketDao, quotaManager, activeTaskRegistry, 100L, 10L);

        when(quotaManager.ensureClaimed(anyString(), eq("upload"), anyLong())).thenReturn(true);

        Optional<Bucket> result = source.nextInput();

        assertThat(result).isPresent();
        assertThat(result.get().getTransferRequests()).containsExactly(tr1, tr2);
    }
}
