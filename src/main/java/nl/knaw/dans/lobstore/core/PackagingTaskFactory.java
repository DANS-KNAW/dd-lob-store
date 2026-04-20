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
import nl.knaw.dans.lobstore.config.DownloadConfig;
import nl.knaw.dans.lobstore.config.PackageConfig;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
public class PackagingTaskFactory implements TaskFactory<TransferRequest> {
    private final BucketDao bucketDao;
    private final TransferRequestDao transferRequestDao;
    private final DownloadConfig downloadConfig;
    private final PackageConfig packageConfig;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final UnitOfWorkAwareProxyFactory unitOfWorkAwareProxyFactory;

    @Override
    public Runnable create(List<TransferRequest> records) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("At least one record is expected");
        }
        UUID bucketId = records.get(0).getBucket().getId();
        return createUnitOfWorkAwareTask(bucketId, bucketDao, transferRequestDao, 
            downloadConfig.getDownloadDirectory(), packageConfig.getUploadDirectory(), 
            packageConfig.getCommand(), quotaManager, activeTaskRegistry);
    }

    private Runnable createUnitOfWorkAwareTask(UUID bucketId, BucketDao bucketDao, TransferRequestDao transferRequestDao, Path downloadDir, Path uploadDir, String packagingCommand, QuotaManager quotaManager, ActiveTaskRegistry activeTaskRegistry) {
        return unitOfWorkAwareProxyFactory.create(PackagingTask.class,
            new Class[] { UUID.class, BucketDao.class, TransferRequestDao.class, Path.class, Path.class, String.class, QuotaManager.class, ActiveTaskRegistry.class },
            new Object[] { bucketId, bucketDao, transferRequestDao, downloadDir, uploadDir, packagingCommand, quotaManager, activeTaskRegistry });
    }
}
