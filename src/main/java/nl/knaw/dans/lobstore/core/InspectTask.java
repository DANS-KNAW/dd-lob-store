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

import io.dropwizard.hibernate.UnitOfWork;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class InspectTask implements Runnable {
    private final UUID transferRequestId;
    private final TransferRequestDao transferRequestDao;
    private final DataverseClient dataverseClient;

    @Override
    @UnitOfWork
    public void run() {
        TransferRequest transferRequest = null;
        try {
            transferRequest = transferRequestDao.findById(transferRequestId).orElseThrow(() -> new RuntimeException("Transfer request with id " + transferRequestId + " not found"));
            var r = dataverseClient.file(transferRequest.getDataverseFileId()).getMetadata();
            String sha1SumDataverse = r.getData().getDataFile().getChecksum().getValue();
            if (sha1SumDataverse.equals(transferRequest.getSha1Sum())) {
                transferRequest.setFileSize(r.getData().getDataFile().getFilesize());
                transferRequest.setStatus(TransferStatus.INSPECTED);
            }
            else {
                transferRequest.setStatus(TransferStatus.REJECTED);
                transferRequest.setMessage("SHA-1 in request " + transferRequest.getSha1Sum() + " does not match SHA-1 in Dataverse " + sha1SumDataverse);
            }
            transferRequestDao.save(transferRequest);
        }
        catch (Exception e) {
            log.error("Error inspecting transfer request with id " + transferRequestId, e);
            if (transferRequest != null) {
                transferRequest.setStatus(TransferStatus.FAILED);
                transferRequest.setMessage("Error inspecting transfer request: " + e.getMessage());
                transferRequestDao.save(transferRequest);
            }

            throw new RuntimeException(e);
        }
    }
}
