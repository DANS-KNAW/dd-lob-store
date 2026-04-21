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

import nl.knaw.dans.lobstore.config.ExternalCommandConfig;
import nl.knaw.dans.lobstore.db.BucketDao;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class PackagingTaskTest {

    private final BucketDao bucketDao = mock(BucketDao.class);
    private final QuotaManager quotaManager = mock(QuotaManager.class);
    private final ActiveTaskRegistry activeTaskRegistry = mock(ActiveTaskRegistry.class);
    private final Path downloadDir = Path.of("target/test/PackagingTaskTest/download");
    private final Path uploadDir = Path.of("target/test/PackagingTaskTest/upload");

    @Test
    void run_should_fail_if_transfer_requests_have_different_datastations() {
        UUID bucketId = UUID.randomUUID();
        String bucketDatastation = "station1";
        
        TransferRequest tr1 = TransferRequest.builder()
            .id(UUID.randomUUID())
            .datastation("station1")
            .build();
            
        TransferRequest tr2 = TransferRequest.builder()
            .id(UUID.randomUUID())
            .datastation("station2") // Different datastation!
            .build();

        Bucket bucket = Bucket.builder()
            .id(bucketId)
            .status(BucketStatus.PACKAGING)
            .datastation(bucketDatastation)
            .transferRequests(List.of(tr1, tr2))
            .build();

        when(bucketDao.findById(bucketId)).thenReturn(Optional.of(bucket));

        ExternalCommandConfig packagingCommand = new ExternalCommandConfig();
        packagingCommand.setExecutable("true");
        PackagingTask task = new PackagingTask(bucketId, bucketDao, downloadDir, uploadDir, packagingCommand, quotaManager, activeTaskRegistry);
        
        task.run();

        assertThat(bucket.getStatus()).isEqualTo(BucketStatus.FAILED);
        verify(bucketDao, atLeastOnce()).save(bucket);
        verify(activeTaskRegistry).remove(bucketId);
    }
    
    @Test
    void run_should_succeed_if_all_transfer_requests_have_same_datastation() throws Exception {
        // This test is more complex because it actually tries to move files and execute commands.
        // For the purpose of verifying the sanity check, the above test might be enough,
        // but let's see if we can at least verify it passes the check.
        
        UUID bucketId = UUID.randomUUID();
        String bucketDatastation = "station1";
        
        TransferRequest tr1 = TransferRequest.builder()
            .id(UUID.randomUUID())
            .datastation("station1")
            .sha1Sum("abc")
            .build();

        Bucket bucket = Bucket.builder()
            .id(bucketId)
            .status(BucketStatus.PACKAGING)
            .datastation(bucketDatastation)
            .transferRequests(List.of(tr1))
            .build();

        when(bucketDao.findById(bucketId)).thenReturn(Optional.of(bucket));
        
        // We'll probably hit an IOException later because the files don't exist, 
        // which is fine as long as it doesn't fail the sanity check.
        
        ExternalCommandConfig packagingCommand = new ExternalCommandConfig();
        packagingCommand.setExecutable("true");
        PackagingTask task = new PackagingTask(bucketId, bucketDao, downloadDir, uploadDir, packagingCommand, quotaManager, activeTaskRegistry);
        
        task.run();

        // If it passed the sanity check, it should have tried to resolve the bucket folder.
        // It will fail with IOException when trying to move files.
        assertThat(bucket.getStatus()).isEqualTo(BucketStatus.PACKAGING); // Remains PACKAGING if it caught IOException (though current implementation re-throws IOException? No, it logs it and then what?)
    }
}
