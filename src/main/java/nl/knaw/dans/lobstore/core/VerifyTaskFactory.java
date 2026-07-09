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
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskFactory;
import nl.knaw.dans.lobstore.config.DataStationConfig;
import nl.knaw.dans.lobstore.config.ExternalCommandConfig;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.LocationDao;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

@RequiredArgsConstructor
public class VerifyTaskFactory implements TaskFactory<Bucket> {
    private final BucketDao bucketDao;
    private final LocationDao locationDao;
    private final ExternalCommandConfig verifyCommand;
    private final String invalidOn;
    private final Map<String, DataStationConfig> datastations;
    private final Path uploadDir;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final MoratoriumManager moratoriumManager;
    private final String connectionRefusedOn;
    private final Duration moratoriumDuration;
    private final UnitOfWorkAwareProxyFactory unitOfWorkAwareProxyFactory;

    @Override
    public Runnable create(Bucket bucket) {
        var proxiedTask = createUnitOfWorkAwareTask(bucket.getId(), bucketDao, locationDao, verifyCommand, invalidOn, datastations, uploadDir, quotaManager, moratoriumManager, connectionRefusedOn, moratoriumDuration);
        return () -> {
            try {
                proxiedTask.run();
            }
            finally {
                activeTaskRegistry.remove(bucket.getId());
            }
        };
    }

    private Runnable createUnitOfWorkAwareTask(UUID bucketId, BucketDao bucketDao, LocationDao locationDao, ExternalCommandConfig verifyCommand, String invalidOn, Map<String, DataStationConfig> datastations, Path uploadDir, QuotaManager quotaManager, MoratoriumManager moratoriumManager, String connectionRefusedOn, Duration moratoriumDuration) {
        return unitOfWorkAwareProxyFactory.create(VerifyTask.class,
            new Class[] { UUID.class, BucketDao.class, LocationDao.class, ExternalCommandConfig.class, String.class, Map.class, Path.class, QuotaManager.class, MoratoriumManager.class, String.class, Duration.class },
            new Object[] { bucketId, bucketDao, locationDao, verifyCommand, invalidOn, datastations, uploadDir, quotaManager, moratoriumManager, connectionRefusedOn, moratoriumDuration });
    }
}
