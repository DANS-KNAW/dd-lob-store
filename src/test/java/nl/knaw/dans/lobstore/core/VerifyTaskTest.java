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
import nl.knaw.dans.lobstore.config.ExternalCommandConfig;
import nl.knaw.dans.lobstore.config.LobStoreConfig;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.LocationDao;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class VerifyTaskTest {

    private final BucketDao bucketDao = mock(BucketDao.class);
    private final LocationDao locationDao = mock(LocationDao.class);
    private final QuotaManager quotaManager = mock(QuotaManager.class);
    private final ActiveTaskRegistry activeTaskRegistry = mock(ActiveTaskRegistry.class);
    private final Path uploadDir = Path.of("target/test/VerifyTaskTest/upload");

    @BeforeEach
    void setUp() throws IOException {
        FileUtils.deleteDirectory(uploadDir.toFile());
        Files.createDirectories(uploadDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(uploadDir.toFile());
    }

    @Test
    void run_should_substitute_variables_and_execute_command_and_cleanup() throws IOException {
        UUID bucketId = UUID.randomUUID();
        String datastationName = "station1";
        
        UUID tr1Id = UUID.randomUUID();
        TransferRequest tr1 = new TransferRequest();
        tr1.setId(tr1Id);
        tr1.setSha1Sum("sha1-1");
        
        Bucket bucket = Bucket.builder()
            .id(bucketId)
            .status(BucketStatus.VERIFYING)
            .datastation(datastationName)
            .transferRequests(List.of(tr1))
            .build();

        when(bucketDao.findById(bucketId)).thenReturn(Optional.of(bucket));

        LobStoreConfig lobstoreConfig = new LobStoreConfig();
        lobstoreConfig.setUser("testuser");
        lobstoreConfig.setHost("testhost");
        lobstoreConfig.setPath(Path.of("/test/path"));

        DataStationConfig dsConfig = new DataStationConfig();
        dsConfig.setLobstore(lobstoreConfig);

        Map<String, DataStationConfig> datastations = Map.of(datastationName, dsConfig);

        Path bucketFile = uploadDir.resolve(bucketId.toString() + ".dmftar");
        Files.createFile(bucketFile);

        ExternalCommandConfig verifyCommand = new ExternalCommandConfig();
        verifyCommand.setExecutable("echo");
        verifyCommand.setArgs(List.of("${bucketname}", "${datastation}", "${user}", "${host}", "${path}"));
        VerifyTask task = new VerifyTask(bucketId, bucketDao, locationDao, verifyCommand, datastations, uploadDir, quotaManager, activeTaskRegistry);

        task.run();

        assertThat(bucket.getStatus()).isEqualTo(BucketStatus.DONE);
        verify(bucketDao).save(bucket);
        verify(locationDao).save(Location.builder()
            .datastation(datastationName)
            .sha1Sum("sha1-1")
            .bucketName(bucketId.toString())
            .build());
        verify(activeTaskRegistry).remove(bucketId);
        verify(quotaManager).release(tr1Id + "/base", "download");
        assertThat(Files.exists(bucketFile)).isFalse();
    }

    @Test
    void run_should_leave_bucket_in_verifying_state_on_failure() throws IOException {
        UUID bucketId = UUID.randomUUID();
        String datastationName = "station1";
        Bucket bucket = Bucket.builder()
            .id(bucketId)
            .status(BucketStatus.VERIFYING)
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

        Path bucketFile = uploadDir.resolve(bucketId.toString() + ".dmftar");
        Files.createFile(bucketFile);

        // Command that fails
        ExternalCommandConfig verifyCommand = new ExternalCommandConfig();
        verifyCommand.setExecutable("false");
        VerifyTask task = new VerifyTask(bucketId, bucketDao, locationDao, verifyCommand, datastations, uploadDir, quotaManager, activeTaskRegistry);

        task.run();

        assertThat(bucket.getStatus()).isEqualTo(BucketStatus.VERIFYING);
        verify(bucketDao, never()).save(any());
        verify(activeTaskRegistry).remove(bucketId);
        assertThat(Files.exists(bucketFile)).isTrue();
    }
}
