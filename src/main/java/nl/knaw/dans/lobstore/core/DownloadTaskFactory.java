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
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskFactory;
import nl.knaw.dans.lobstore.config.DownloadConfig;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

@RequiredArgsConstructor
public class DownloadTaskFactory implements TaskFactory<TransferRequest> {
    private final TransferRequestDao transferRequestDao;
    private final Map<String, DataverseClient> dataverseClients;
    private final DownloadConfig downloadConfig;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final UnitOfWorkAwareProxyFactory unitOfWorkAwareProxyFactory;
    private final ExecutorService chunkDownloadExecutor;

    @Override
    public Runnable create(List<TransferRequest> records) {
        if (records.size() != 1) {
            throw new IllegalArgumentException("Exactly one record is expected, but got " + records.size());
        }
        var transferRequest = records.get(0);
        return createUnitOfWorkAwareTask(transferRequest.getId(), transferRequestDao,
            dataverseClients.get(transferRequest.getDatastation()), downloadConfig);
    }

    private Runnable createUnitOfWorkAwareTask(UUID id, TransferRequestDao transferRequestDao, DataverseClient dataverseClient, DownloadConfig downloadConfig) {
        return unitOfWorkAwareProxyFactory.create(DownloadTask.class,
            new Class[] { UUID.class, TransferRequestDao.class, DataverseClient.class, DownloadConfig.class, QuotaManager.class, ActiveTaskRegistry.class, ExecutorService.class },
            new Object[] { id, transferRequestDao, dataverseClient, downloadConfig, quotaManager, activeTaskRegistry, chunkDownloadExecutor });
    }
}
