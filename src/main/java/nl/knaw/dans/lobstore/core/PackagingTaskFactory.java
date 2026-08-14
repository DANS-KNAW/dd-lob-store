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
import io.dropwizard.util.DataSize;
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskFactory;
import nl.knaw.dans.lobstore.config.DownloadConfig;
import nl.knaw.dans.lobstore.config.ExternalCommandConfig;
import nl.knaw.dans.lobstore.config.PackageConfig;
import nl.knaw.dans.lobstore.db.BucketDao;

import java.nio.file.Path;
import java.util.UUID;

@RequiredArgsConstructor
public class PackagingTaskFactory implements TaskFactory<Bucket> {
    private final BucketDao bucketDao;
    private final DownloadConfig downloadConfig;
    private final PackageConfig packageConfig;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final UnitOfWorkAwareProxyFactory unitOfWorkAwareProxyFactory;

    @Override
    public Runnable create(Bucket bucket) {
        var proxiedTask = createUnitOfWorkAwareTask(bucket.getId(), bucketDao,
            downloadConfig.getDownloadDirectory(), packageConfig.getUploadDirectory(),
            packageConfig.getCommand(), quotaManager, packageConfig.getMinimalBucketSize());
        return () -> {
            try {
                proxiedTask.run();
            }
            finally {
                activeTaskRegistry.remove(bucket.getId());
            }
        };
    }

    private Runnable createUnitOfWorkAwareTask(UUID bucketId, BucketDao bucketDao, Path downloadDir, Path uploadDir, ExternalCommandConfig packagingCommand, QuotaManager quotaManager,
        DataSize minimalBucketSize) {
        return unitOfWorkAwareProxyFactory.create(PackagingTask.class,
            new Class[] { UUID.class, BucketDao.class, Path.class, Path.class, ExternalCommandConfig.class, QuotaManager.class, DataSize.class },
            new Object[] { bucketId, bucketDao, downloadDir, uploadDir, packagingCommand, quotaManager, minimalBucketSize });
    }
}
