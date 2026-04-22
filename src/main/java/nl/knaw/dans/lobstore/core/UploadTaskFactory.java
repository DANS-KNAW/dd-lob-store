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

import java.util.Map;
import java.util.UUID;

@RequiredArgsConstructor
public class UploadTaskFactory implements TaskFactory<Bucket> {
    private final BucketDao bucketDao;
    private final ExternalCommandConfig uploadCommand;
    private final Map<String, DataStationConfig> datastations;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final UnitOfWorkAwareProxyFactory unitOfWorkAwareProxyFactory;

    @Override
    public Runnable create(Bucket bucket) {
        return createUnitOfWorkAwareTask(bucket.getId(), bucketDao, uploadCommand, datastations, activeTaskRegistry);
    }

    private Runnable createUnitOfWorkAwareTask(UUID bucketId, BucketDao bucketDao, ExternalCommandConfig uploadCommand, Map<String, DataStationConfig> datastations, ActiveTaskRegistry activeTaskRegistry) {
        return unitOfWorkAwareProxyFactory.create(UploadTask.class,
            new Class[] { UUID.class, BucketDao.class, ExternalCommandConfig.class, Map.class, ActiveTaskRegistry.class },
            new Object[] { bucketId, bucketDao, uploadCommand, datastations, activeTaskRegistry });
    }
}
