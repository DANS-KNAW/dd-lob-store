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
import nl.knaw.dans.lib.dataverse.BasicFileAccessApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lobstore.config.DownloadConfig;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.apache.commons.io.FileUtils;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class DownloadTask implements Runnable {
    private final UUID transferRequestId;
    private final TransferRequestDao transferRequestDao;
    private final DataverseClient dataverseClient;
    private final DownloadConfig downloadConfig;
    private final QuotaManager quotaManager;

    @Override
    @UnitOfWork
    public void run() {
        log.info("Starting DOWNLOAD step for {}", transferRequestId);
        TransferRequest transferRequest = null;
        try {
            transferRequest = transferRequestDao.findById(transferRequestId)
                .orElseThrow(() -> new RuntimeException("Transfer request with id " + transferRequestId + " not found"));
            
            BasicFileAccessApi basicFileAccessApi = dataverseClient.basicFileAccess(transferRequest.getDataverseFileId());
            Path outputFile = downloadConfig.getDownloadDirectory().resolve(transferRequest.getId().toString());

            basicFileAccessApi.getFile(response -> {
                try (InputStream is = response.getEntity().getContent()) {
                    FileUtils.copyInputStreamToFile(is, outputFile.toFile());
                }
                return null;
            });

            transferRequest.setStatus(TransferStatus.DOWNLOADED);
            transferRequestDao.save(transferRequest);
            quotaManager.release(transferRequest.getId() + "/2", "download");
            log.info("Finished DOWNLOAD step for {}", transferRequestId);
        }
        catch (Exception e) {
            log.error("Error downloading file for transfer request with id {}", transferRequestId, e);
            if (transferRequest != null) {
                transferRequest.setStatus(TransferStatus.FAILED);
                transferRequest.setMessage("Error downloading file: " + e.getMessage());
                transferRequestDao.save(transferRequest);
            }
            throw new RuntimeException(e);
        }
    }
}
