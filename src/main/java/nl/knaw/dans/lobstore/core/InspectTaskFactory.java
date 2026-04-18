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
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RequiredArgsConstructor
public class InspectTaskFactory implements TaskFactory<TransferRequest> {
    private final TransferRequestDao transferRequestDao;
    private final Map<String, DataverseClient> dataverseClients;
    private final UnitOfWorkAwareProxyFactory unitOfWorkAwareProxyFactory;

    @Override
    public Runnable create(List<TransferRequest> records) {
        if (records.size() != 1) {
            throw new IllegalArgumentException("Exactly one record is expected, but got " + records.size());
        }
        var transferRequest = records.get(0);
        return createUnitOfWorkAwareTask(transferRequest.getId(), transferRequestDao, dataverseClients.get(transferRequest.getDatastation()));
    }

    private Runnable createUnitOfWorkAwareTask(UUID id, TransferRequestDao transferRequestDao, DataverseClient dataverseClient) {
        return unitOfWorkAwareProxyFactory.create(InspectTask.class,
            new Class[] { UUID.class, TransferRequestDao.class, DataverseClient.class },
            new Object[] { id, transferRequestDao, dataverseClient });
    }

}
