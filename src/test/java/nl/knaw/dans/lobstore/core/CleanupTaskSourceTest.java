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
import nl.knaw.dans.lobstore.db.LocationDao;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CleanupTaskSourceTest {

    private final BucketDao bucketDao = mock(BucketDao.class);
    private final LocationDao locationDao = mock(LocationDao.class);
    private final ActiveTaskRegistry activeTaskRegistry = mock(ActiveTaskRegistry.class);
    private final CleanupTaskSource source = new CleanupTaskSource(bucketDao, locationDao, activeTaskRegistry);

    @Test
    void nextInput_should_return_first_available_item() {
        UUID id1 = UUID.randomUUID();
        Bucket bucket1 = Bucket.builder().id(id1).status(BucketStatus.DONE).build();

        when(bucketDao.findByStatus(BucketStatus.DONE, 10)).thenReturn(List.of(bucket1));
        when(locationDao.countByBucketName(id1.toString())).thenReturn(1L);
        when(activeTaskRegistry.add(id1)).thenReturn(true);

        Optional<Bucket> result = source.nextInput();

        assertThat(result).contains(bucket1);
        verify(activeTaskRegistry).add(id1);
    }

    @Test
    void nextInput_should_skip_active_items() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        Bucket bucket1 = Bucket.builder().id(id1).status(BucketStatus.DONE).build();
        Bucket bucket2 = Bucket.builder().id(id2).status(BucketStatus.DONE).build();

        when(bucketDao.findByStatus(BucketStatus.DONE, 10)).thenReturn(List.of(bucket1, bucket2));
        when(locationDao.countByBucketName(id1.toString())).thenReturn(1L);
        when(locationDao.countByBucketName(id2.toString())).thenReturn(1L);
        when(activeTaskRegistry.add(id1)).thenReturn(false);
        when(activeTaskRegistry.add(id2)).thenReturn(true);

        Optional<Bucket> result = source.nextInput();

        assertThat(result).contains(bucket2);
        verify(activeTaskRegistry).add(id1);
        verify(activeTaskRegistry).add(id2);
    }

    @Test
    void nextInput_should_mark_as_failed_if_no_location() {
        UUID id1 = UUID.randomUUID();
        Bucket bucket1 = Bucket.builder().id(id1).status(BucketStatus.DONE).build();

        when(bucketDao.findByStatus(BucketStatus.DONE, 10)).thenReturn(List.of(bucket1));
        when(locationDao.countByBucketName(id1.toString())).thenReturn(0L);

        Optional<Bucket> result = source.nextInput();

        assertThat(result).isEmpty();
        assertThat(bucket1.getStatus()).isEqualTo(BucketStatus.FAILED);
        verify(bucketDao).save(bucket1);
        verify(activeTaskRegistry, never()).add(id1);
    }

    @Test
    void nextInput_should_return_empty_if_no_items() {
        when(bucketDao.findByStatus(BucketStatus.DONE, 10)).thenReturn(List.of());

        Optional<Bucket> result = source.nextInput();

        assertThat(result).isEmpty();
    }
}
