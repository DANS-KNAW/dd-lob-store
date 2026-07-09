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
import nl.knaw.dans.lobstore.config.ExternalCommandConfig;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.LocationDao;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.*;

class VerifyTaskFactoryTest {

    @Test
    void create_should_return_runnable_that_removes_from_registry_after_execution() {
        BucketDao bucketDao = mock(BucketDao.class);
        LocationDao locationDao = mock(LocationDao.class);
        ExternalCommandConfig verifyCommand = mock(ExternalCommandConfig.class);
        ActiveTaskRegistry activeTaskRegistry = mock(ActiveTaskRegistry.class);
        MoratoriumManager moratoriumManager = mock(MoratoriumManager.class);
        UnitOfWorkAwareProxyFactory uowFactory = mock(UnitOfWorkAwareProxyFactory.class);
        
        VerifyTask proxiedTask = mock(VerifyTask.class);
        when(uowFactory.create(eq(VerifyTask.class), any(Class[].class), any(Object[].class))).thenReturn(proxiedTask);

        VerifyTaskFactory factory = new VerifyTaskFactory(bucketDao, locationDao, verifyCommand, "invalid", Map.of(), Path.of("upload"), mock(QuotaManager.class), activeTaskRegistry, moratoriumManager, "refused", Duration.ZERO, uowFactory);
        
        UUID bucketId = UUID.randomUUID();
        Bucket bucket = Bucket.builder().id(bucketId).build();
        
        Runnable task = factory.create(bucket);
        task.run();
        
        verify(proxiedTask).run();
        verify(activeTaskRegistry).remove(bucketId);
    }
}
