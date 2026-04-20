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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class UploadTaskSourceTest {

    private final BucketDao bucketDao = mock(BucketDao.class);
    private final ActiveTaskRegistry activeTaskRegistry = mock(ActiveTaskRegistry.class);
    private final UploadTaskSource source = new UploadTaskSource(bucketDao, activeTaskRegistry);

    @Test
    void nextInputs_should_return_interrupted_uploading_bucket() {
        UUID bucketId = UUID.randomUUID();
        Bucket bucket = Bucket.builder().id(bucketId).status(BucketStatus.UPLOADING).build();

        when(bucketDao.findByStatus(BucketStatus.UPLOADING)).thenReturn(List.of(bucket));
        when(activeTaskRegistry.contains(bucketId)).thenReturn(false);

        List<Bucket> result = source.nextInputs();

        assertThat(result).containsExactly(bucket);
        verify(activeTaskRegistry).add(bucketId);
    }

    @Test
    void nextInputs_should_return_packaged_bucket_and_set_to_uploading() {
        UUID bucketId = UUID.randomUUID();
        Bucket bucket = Bucket.builder().id(bucketId).status(BucketStatus.PACKAGED).build();

        when(bucketDao.findByStatus(BucketStatus.UPLOADING)).thenReturn(List.of());
        when(bucketDao.findByStatus(BucketStatus.PACKAGED)).thenReturn(List.of(bucket));
        when(activeTaskRegistry.contains(bucketId)).thenReturn(false);

        List<Bucket> result = source.nextInputs();

        assertThat(result).containsExactly(bucket);
        assertThat(bucket.getStatus()).isEqualTo(BucketStatus.UPLOADING);
        verify(bucketDao).save(bucket);
        verify(activeTaskRegistry).add(bucketId);
    }

    @Test
    void nextInputs_should_return_empty_if_all_active() {
        UUID bucketId = UUID.randomUUID();
        Bucket bucket = Bucket.builder().id(bucketId).status(BucketStatus.UPLOADING).build();

        when(bucketDao.findByStatus(BucketStatus.UPLOADING)).thenReturn(List.of(bucket));
        when(activeTaskRegistry.contains(bucketId)).thenReturn(true);
        when(bucketDao.findByStatus(BucketStatus.PACKAGED)).thenReturn(List.of());

        List<Bucket> result = source.nextInputs();

        assertThat(result).isEmpty();
    }
}
