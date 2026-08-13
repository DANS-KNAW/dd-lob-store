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

import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.ClaimDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.UUID;

import static org.mockito.Mockito.*;

class CleanupTaskFactoryTest {

    @Test
    void create_should_return_runnable_that_removes_from_registry_after_execution() {
        BucketDao bucketDao = mock(BucketDao.class);
        TransferRequestDao transferRequestDao = mock(TransferRequestDao.class);
        ClaimDao claimDao = mock(ClaimDao.class);
        ActiveTaskRegistry activeTaskRegistry = mock(ActiveTaskRegistry.class);
        UnitOfWorkAwareProxyFactory uowFactory = mock(UnitOfWorkAwareProxyFactory.class);
        
        CleanupTask proxiedTask = mock(CleanupTask.class);
        when(uowFactory.create(eq(CleanupTask.class), any(Class[].class), any(Object[].class))).thenReturn(proxiedTask);

        CleanupTaskFactory factory = new CleanupTaskFactory(bucketDao, transferRequestDao, claimDao, Path.of("upload"), Path.of("download"), activeTaskRegistry, uowFactory);
        
        UUID bucketId = UUID.randomUUID();
        Bucket bucket = Bucket.builder().id(bucketId).build();
        
        Runnable task = factory.create(bucket);
        task.run();
        
        verify(proxiedTask).run();
        verify(activeTaskRegistry).remove(bucketId);
    }
}
