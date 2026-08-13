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
import nl.knaw.dans.lobstore.db.ClaimDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class CleanupTaskTest {

    private final BucketDao bucketDao = mock(BucketDao.class);
    private final TransferRequestDao transferRequestDao = mock(TransferRequestDao.class);
    private final ClaimDao claimDao = mock(ClaimDao.class);

    @TempDir
    Path tempUploadDir;

    @TempDir
    Path tempDownloadDir;

    @Test
    void run_should_cleanup_bucket_and_transfers() throws Exception {
        UUID bucketId = UUID.randomUUID();
        UUID trId1 = UUID.randomUUID();
        UUID trId2 = UUID.randomUUID();
        
        TransferRequest tr1 = TransferRequest.builder().id(trId1).build();
        TransferRequest tr2 = TransferRequest.builder().id(trId2).build();
        
        Bucket bucket = Bucket.builder()
            .id(bucketId)
            .transferRequests(List.of(tr1, tr2))
            .build();
            
        when(bucketDao.findById(bucketId)).thenReturn(Optional.of(bucket));
        
        // create dummy directories and files
        Path trDir1 = tempDownloadDir.resolve(trId1.toString());
        Files.createDirectories(trDir1);
        Files.writeString(trDir1.resolve("file.txt"), "data");
        
        Path bucketFolder = tempUploadDir.resolve(bucketId.toString());
        Files.createDirectories(bucketFolder);
        Files.writeString(bucketFolder.resolve("test.txt"), "data");
        
        Path bucketTar = tempUploadDir.resolve(bucketId.toString() + ".dmftar");
        Files.createDirectories(bucketTar);
        Files.writeString(bucketTar.resolve("content.txt"), "data");
        
        CleanupTask task = new CleanupTask(bucketId, bucketDao, transferRequestDao, claimDao, tempUploadDir, tempDownloadDir);
        task.run();
        
        assertThat(trDir1).doesNotExist();
        assertThat(bucketFolder).doesNotExist();
        assertThat(bucketTar).doesNotExist();
        
        verify(claimDao).deleteByIdStartingWith(trId1.toString());
        verify(claimDao).deleteByIdStartingWith(trId2.toString());
        verify(transferRequestDao).delete(tr1);
        verify(transferRequestDao).delete(tr2);
        
        verify(claimDao).deleteByIdStartingWith(bucketId.toString());
        verify(bucketDao).delete(bucket);
    }
    
    @Test
    void run_should_do_nothing_if_bucket_not_found() throws Exception {
        UUID bucketId = UUID.randomUUID();
        when(bucketDao.findById(bucketId)).thenReturn(Optional.empty());
        
        CleanupTask task = new CleanupTask(bucketId, bucketDao, transferRequestDao, claimDao, tempUploadDir, tempDownloadDir);
        task.run();
        
        verify(claimDao, never()).deleteByIdStartingWith(anyString());
        verify(bucketDao, never()).delete(any());
    }
}
