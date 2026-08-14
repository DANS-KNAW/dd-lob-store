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
package nl.knaw.dans.lobstore.resources;

import nl.knaw.dans.lobstore.core.ActiveTaskRegistry;
import nl.knaw.dans.lobstore.core.Bucket;
import nl.knaw.dans.lobstore.core.BucketStatus;
import nl.knaw.dans.lobstore.core.QuotaManager;
import nl.knaw.dans.lobstore.core.TransferRequest;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
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

class DatastationsResourceTest {

    private final TransferRequestDao transferRequestDao = mock(TransferRequestDao.class);
    private final BucketDao bucketDao = mock(BucketDao.class);
    private final QuotaManager quotaManager = mock(QuotaManager.class);

    private DatastationsResource resource;

    @BeforeEach
    void setUp() {
        long minimalBucketSize = 1000L;
        long margin = 100L;
        resource = new DatastationsResource(transferRequestDao, bucketDao, quotaManager, minimalBucketSize, margin);
    }

    @Test
    void flushTransfers_should_return_204_when_no_items_found() {
        when(transferRequestDao.findPackagableItemsByDatastation("station1")).thenReturn(Collections.emptyList());

        Response response = resource.flushTransfers("station1");

        assertThat(response.getStatus()).isEqualTo(204);
        verify(quotaManager, never()).ensureClaimed(anyString(), anyString(), anyLong());
    }

    @Test
    void flushTransfers_should_return_202_and_create_bucket_when_quota_granted() {
        TransferRequest item1 = TransferRequest.builder().id(UUID.randomUUID()).fileSize(400L).build();
        TransferRequest item2 = TransferRequest.builder().id(UUID.randomUUID()).fileSize(200L).build();

        when(transferRequestDao.findPackagableItemsByDatastation("station1")).thenReturn(List.of(item1, item2));

        // Total size is 600, minimal is 1000, so claim should be for 1000 and 1100.
        when(quotaManager.ensureClaimed(anyString(), eq("upload"), eq(1000L))).thenReturn(true);
        when(quotaManager.ensureClaimed(anyString(), eq("upload"), eq(1100L))).thenReturn(true);

        Response response = resource.flushTransfers("station1");

        assertThat(response.getStatus()).isEqualTo(202);
        verify(bucketDao).save(any(Bucket.class));
        verify(transferRequestDao).save(item1);
        verify(transferRequestDao).save(item2);

        assertThat(item1.getBucket().getStatus()).isEqualTo(BucketStatus.PACKAGING);
    }

    @Test
    void flushTransfers_should_return_503_when_quota_base_claim_denied() {
        TransferRequest item1 = TransferRequest.builder().id(UUID.randomUUID()).fileSize(1500L).build();

        when(transferRequestDao.findPackagableItemsByDatastation("station1")).thenReturn(List.of(item1));

        // Total size is 1500, minimal is 1000, so claim should be for 1500.
        when(quotaManager.ensureClaimed(anyString(), eq("upload"), eq(1500L))).thenReturn(false);

        Response response = resource.flushTransfers("station1");

        assertThat(response.getStatus()).isEqualTo(503);
        verify(bucketDao, never()).save(any(Bucket.class));
    }

    @Test
    void flushTransfers_should_release_base_claim_and_return_503_when_extra_claim_denied() {
        TransferRequest item1 = TransferRequest.builder().id(UUID.randomUUID()).fileSize(500L).build();

        when(transferRequestDao.findPackagableItemsByDatastation("station1")).thenReturn(List.of(item1));

        when(quotaManager.ensureClaimed(anyString(), eq("upload"), eq(1000L))).thenReturn(true);
        when(quotaManager.ensureClaimed(anyString(), eq("upload"), eq(1100L))).thenReturn(false);

        Response response = resource.flushTransfers("station1");

        assertThat(response.getStatus()).isEqualTo(503);
        verify(quotaManager).release(anyString(), eq("upload"));
        verify(bucketDao, never()).save(any(Bucket.class));
    }
}
