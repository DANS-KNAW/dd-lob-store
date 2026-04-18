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

import io.dropwizard.util.DataSize;
import nl.knaw.dans.lobstore.db.ClaimDao;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QuotaManagerTest {

    private final ClaimDao claimDao = mock(ClaimDao.class);
    private final Map<String, DataSize> diskSpace = Map.of(
        "download", DataSize.gigabytes(10),
        "upload", DataSize.gigabytes(20)
    );
    private final QuotaManager quotaManager = new QuotaManager(claimDao, diskSpace);

    @Test
    void claim_should_return_true_if_space_available() {
        String id = "test-id";
        String target = "download";
        long size = 1024L;

        when(claimDao.findById(id)).thenReturn(Optional.empty());
        when(claimDao.sumSizeByTarget(target)).thenReturn(0L);

        boolean result = quotaManager.claim(id, target, size);

        assertThat(result).isTrue();
        verify(claimDao).save(any(Claim.class));
    }

    @Test
    void claim_should_return_false_if_id_already_exists() {
        String id = "test-id";
        String target = "download";
        long size = 1024L;

        when(claimDao.findById(id)).thenReturn(Optional.of(new Claim(id, target, size)));

        boolean result = quotaManager.claim(id, target, size);

        assertThat(result).isFalse();
        verify(claimDao, never()).save(any(Claim.class));
    }

    @Test
    void claim_should_throw_IllegalArgumentException_if_target_quota_not_configured() {
        String id = "test-id";
        String target = "non-existent";
        long size = 1024L;

        when(claimDao.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> quotaManager.claim(id, target, size))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No quota configured for target 'non-existent'");

        verify(claimDao, never()).save(any(Claim.class));
    }

    @Test
    void claim_should_return_false_if_not_enough_space() {
        String id = "test-id";
        String target = "download";
        long size = DataSize.gigabytes(11).toBytes();

        when(claimDao.findById(id)).thenReturn(Optional.empty());
        when(claimDao.sumSizeByTarget(target)).thenReturn(0L);

        boolean result = quotaManager.claim(id, target, size);

        assertThat(result).isFalse();
        verify(claimDao, never()).save(any(Claim.class));
    }

    @Test
    void claim_should_consider_existing_claims_when_checking_space() {
        String id = "test-id";
        String target = "download";
        long size = DataSize.gigabytes(5).toBytes();

        when(claimDao.findById(id)).thenReturn(Optional.empty());
        when(claimDao.sumSizeByTarget(target)).thenReturn(DataSize.gigabytes(6).toBytes());

        boolean result = quotaManager.claim(id, target, size);

        assertThat(result).isFalse();
        verify(claimDao, never()).save(any(Claim.class));
    }

    @Test
    void release_should_delete_claim_if_found_and_target_matches() {
        String id = "test-id";
        String target = "download";
        Claim claim = new Claim(id, target, 1024L);

        when(claimDao.findById(id)).thenReturn(Optional.of(claim));

        quotaManager.release(id, target);

        verify(claimDao).delete(claim);
    }

    @Test
    void release_should_not_delete_claim_if_target_mismatch() {
        String id = "test-id";
        String target = "download";
        Claim claim = new Claim(id, "upload", 1024L);

        when(claimDao.findById(id)).thenReturn(Optional.of(claim));

        quotaManager.release(id, target);

        verify(claimDao, never()).delete(any(Claim.class));
    }

    @Test
    void release_should_throw_IllegalArgumentException_if_target_quota_not_configured() {
        String id = "test-id";
        String target = "non-existent";

        assertThatThrownBy(() -> quotaManager.release(id, target))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No quota configured for target 'non-existent'");

        verify(claimDao, never()).delete(any(Claim.class));
    }
}
