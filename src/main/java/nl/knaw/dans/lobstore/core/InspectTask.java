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
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.hibernate.HibernateException;

import java.io.IOException;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class InspectTask implements Runnable {
    private final UUID transferRequestId;
    private final TransferRequestDao transferRequestDao;
    private final DataverseClient dataverseClient;
    private final ActiveTaskRegistry activeTaskRegistry;

    @Override
    @UnitOfWork
    public void run() {
        log.info("Start INSPECT step for {}", transferRequestId);
        TransferRequest transferRequest = null;
        try {
            transferRequest = transferRequestDao.findById(transferRequestId).orElseThrow(() -> new RuntimeException("Transfer request with id " + transferRequestId + " not found"));
            var r = dataverseClient.file(transferRequest.getDataverseFileId()).getMetadata();
            String sha1SumDataverse = r.getData().getDataFile().getChecksum().getValue();
            if (sha1SumDataverse.equals(transferRequest.getSha1Sum())) {
                transferRequest.setFileSize(r.getData().getDataFile().getFilesize());
                transferRequest.setStatus(TransferRequestStatus.INSPECTED);
            }
            else {
                transferRequest.setStatus(TransferRequestStatus.REJECTED);
                transferRequest.setMessage("SHA-1 in request " + transferRequest.getSha1Sum() + " does not match SHA-1 in Dataverse " + sha1SumDataverse);
            }
            transferRequestDao.save(transferRequest);
            log.info("Finished INSPECT step for {}", transferRequestId);
        }
        catch (DataverseException e) {
            if (isRecoverable(e.getStatus())) {
                log.warn("Recoverable Dataverse error ({}) for {}: {}, will retry", e.getStatus(), transferRequestId, e.getMessage());
            }
            else {
                log.error("Permanent Dataverse error ({}) for {}: {}", e.getStatus(), transferRequestId, e.getMessage());
                handleFailure(transferRequest, e);
            }
        }
        catch (IOException | HibernateException e) {
            log.warn("Transient error occurred during inspection for {}: {}, will retry later", transferRequestId, e.getMessage());
        }
        catch (Exception e) {
            if (isInterrupted(e)) {
                log.warn("Inspect task for {} was interrupted", transferRequestId);
                Thread.currentThread().interrupt();
            }
            else {
                log.error("Error inspecting transfer request with id {}", transferRequestId, e);
                handleFailure(transferRequest, e);
                throw new RuntimeException(e);
            }
        }
        finally {
            activeTaskRegistry.remove(transferRequestId);
        }
    }

    private boolean isInterrupted(Throwable e) {
        if (e instanceof InterruptedException) {
            return true;
        }
        if (e.getCause() != null && e.getCause() != e) {
            return isInterrupted(e.getCause());
        }
        return false;
    }

    private boolean isRecoverable(int status) {
        return status == 429 || status == 502 || status == 503 || status == 504;
    }

    private void handleFailure(TransferRequest transferRequest, Exception e) {
        if (transferRequest != null) {
            transferRequest.setStatus(TransferRequestStatus.FAILED);
            transferRequest.setMessage("Error inspecting transfer request: " + e.getMessage());
            transferRequestDao.save(transferRequest);
        }
    }
}
