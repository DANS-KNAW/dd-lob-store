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

import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DownloadTaskSourceTest {

    private static final int MAX_CONCURRENT = 3;

    private final TransferRequestDao transferRequestDao = mock(TransferRequestDao.class);
    private final QuotaManager quotaManager = mock(QuotaManager.class);
    private final ActiveTaskRegistry activeTaskRegistry = new ActiveTaskRegistry();

    private DownloadTaskSource source() {
        return new DownloadTaskSource(transferRequestDao, quotaManager, activeTaskRegistry, 100L, MAX_CONCURRENT);
    }

    private TransferRequest item() {
        return TransferRequest.builder().id(UUID.randomUUID()).fileSize(1000L).datastation("station1").build();
    }

    @Test
    void nextInputs_should_schedule_batch_and_mark_them_downloading() {
        TransferRequest a = item();
        TransferRequest b = item();
        when(transferRequestDao.findDownloadableItems(MAX_CONCURRENT)).thenReturn(List.of(a, b));
        when(quotaManager.ensureClaimed(anyString(), eq("download"), anyLong())).thenReturn(true);

        var result = source().nextInputs();

        assertThat(result).containsExactly(a, b);
        assertThat(a.getStatus()).isEqualTo(TransferRequestStatus.DOWNLOADING);
        assertThat(b.getStatus()).isEqualTo(TransferRequestStatus.DOWNLOADING);
        assertThat(activeTaskRegistry.contains(a.getId())).isTrue();
        assertThat(activeTaskRegistry.contains(b.getId())).isTrue();
        verify(transferRequestDao).save(a);
        verify(transferRequestDao).save(b);
    }

    @Test
    void nextInputs_should_skip_items_already_active() {
        TransferRequest active = item();
        TransferRequest fresh = item();
        // 'active' is already being processed (e.g. an in-flight download at the head of the window).
        activeTaskRegistry.add(active.getId());
        when(transferRequestDao.findDownloadableItems(MAX_CONCURRENT)).thenReturn(List.of(active, fresh));
        when(quotaManager.ensureClaimed(anyString(), eq("download"), anyLong())).thenReturn(true);

        var result = source().nextInputs();

        assertThat(result).containsExactly(fresh);
        assertThat(active.getStatus()).isNull();
        assertThat(fresh.getStatus()).isEqualTo(TransferRequestStatus.DOWNLOADING);
    }

    @Test
    void nextInputs_should_skip_item_whose_quota_claim_fails_and_free_the_registry() {
        TransferRequest a = item();
        when(transferRequestDao.findDownloadableItems(MAX_CONCURRENT)).thenReturn(List.of(a));
        // Base claim fails.
        when(quotaManager.ensureClaimed(anyString(), eq("download"), anyLong())).thenReturn(false);

        var result = source().nextInputs();

        assertThat(result).isEmpty();
        assertThat(a.getStatus()).isNull();
        // Removed from the registry so it can be retried on a later poll.
        assertThat(activeTaskRegistry.contains(a.getId())).isFalse();
    }

    @Test
    void nextInputs_should_return_empty_when_no_downloadable_items() {
        when(transferRequestDao.findDownloadableItems(MAX_CONCURRENT)).thenReturn(List.of());

        assertThat(source().nextInputs()).isEmpty();
    }
}
