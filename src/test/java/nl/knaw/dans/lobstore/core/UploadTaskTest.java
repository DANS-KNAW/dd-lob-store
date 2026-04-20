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

import nl.knaw.dans.lobstore.config.DataStationConfig;
import nl.knaw.dans.lobstore.config.LobStoreConfig;
import nl.knaw.dans.lobstore.db.BucketDao;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class UploadTaskTest {

    private final BucketDao bucketDao = mock(BucketDao.class);
    private final ActiveTaskRegistry activeTaskRegistry = mock(ActiveTaskRegistry.class);

    @Test
    void run_should_substitute_variables_and_execute_command() {
        UUID bucketId = UUID.randomUUID();
        String datastationName = "station1";
        Bucket bucket = Bucket.builder()
            .id(bucketId)
            .status(BucketStatus.UPLOADING)
            .datastation(datastationName)
            .build();

        when(bucketDao.findById(bucketId)).thenReturn(Optional.of(bucket));

        LobStoreConfig lobstoreConfig = new LobStoreConfig();
        lobstoreConfig.setUser("testuser");
        lobstoreConfig.setHost("testhost");
        lobstoreConfig.setPath(Path.of("/test/path"));

        DataStationConfig dsConfig = new DataStationConfig();
        dsConfig.setLobstore(lobstoreConfig);

        Map<String, DataStationConfig> datastations = Map.of(datastationName, dsConfig);

        // We use "true" as the command so it always succeeds on Linux/macOS
        String uploadCommand = "echo ${bucketname} ${datastation} ${user} ${host} ${path}";
        UploadTask task = new UploadTask(bucketId, bucketDao, uploadCommand, datastations, activeTaskRegistry);

        task.run();

        assertThat(bucket.getStatus()).isEqualTo(BucketStatus.UPLOADED);
        verify(bucketDao).save(bucket);
        verify(activeTaskRegistry).remove(bucketId);
    }

    @Test
    void run_should_leave_bucket_in_uploading_state_on_failure() {
        UUID bucketId = UUID.randomUUID();
        String datastationName = "station1";
        Bucket bucket = Bucket.builder()
            .id(bucketId)
            .status(BucketStatus.UPLOADING)
            .datastation(datastationName)
            .build();

        when(bucketDao.findById(bucketId)).thenReturn(Optional.of(bucket));

        LobStoreConfig lobstoreConfig = new LobStoreConfig();
        lobstoreConfig.setUser("testuser");
        lobstoreConfig.setHost("testhost");
        lobstoreConfig.setPath(Path.of("/test/path"));

        DataStationConfig dsConfig = new DataStationConfig();
        dsConfig.setLobstore(lobstoreConfig);

        Map<String, DataStationConfig> datastations = Map.of(datastationName, dsConfig);

        // Command that fails
        String uploadCommand = "false";
        UploadTask task = new UploadTask(bucketId, bucketDao, uploadCommand, datastations, activeTaskRegistry);

        task.run();

        assertThat(bucket.getStatus()).isEqualTo(BucketStatus.UPLOADING);
        verify(bucketDao, never()).save(any());
        verify(activeTaskRegistry).remove(bucketId);
    }
}
