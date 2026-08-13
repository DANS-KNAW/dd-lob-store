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
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.ClaimDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import java.nio.file.Path;
import java.util.UUID;

@RequiredArgsConstructor
public class CleanupTaskFactory implements TaskFactory<Bucket> {
    private final BucketDao bucketDao;
    private final TransferRequestDao transferRequestDao;
    private final ClaimDao claimDao;
    private final Path uploadDir;
    private final Path downloadDir;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final UnitOfWorkAwareProxyFactory unitOfWorkAwareProxyFactory;

    @Override
    public Runnable create(Bucket bucket) {
        var proxiedTask = unitOfWorkAwareProxyFactory.create(CleanupTask.class,
            new Class<?>[] { UUID.class, BucketDao.class, TransferRequestDao.class, ClaimDao.class, Path.class, Path.class },
            new Object[] { bucket.getId(), bucketDao, transferRequestDao, claimDao, uploadDir, downloadDir });

        return () -> {
            try {
                proxiedTask.run();
            }
            finally {
                activeTaskRegistry.remove(bucket.getId());
            }
        };
    }
}
