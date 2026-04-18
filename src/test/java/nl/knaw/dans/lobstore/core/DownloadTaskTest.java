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

import nl.knaw.dans.lib.dataverse.BasicFileAccessApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lobstore.config.DownloadConfig;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DownloadTaskTest {

    private final TransferRequestDao dao = mock(TransferRequestDao.class);
    private final DataverseClient dataverseClient = mock(DataverseClient.class);
    private final BasicFileAccessApi basicFileAccessApi = mock(BasicFileAccessApi.class);
    private final DownloadConfig downloadConfig = new DownloadConfig();
    private final QuotaManager quotaManager = mock(QuotaManager.class);

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        downloadConfig.setDownloadDirectory(tempDir);
        when(dataverseClient.basicFileAccess(anyLong())).thenReturn(basicFileAccessApi);
    }

    @Test
    void run_should_download_file_and_update_status() throws Exception {
        UUID id = UUID.randomUUID();
        TransferRequest request = TransferRequest.builder()
            .id(id)
            .dataverseFileId(123L)
            .status(TransferStatus.INSPECTED)
            .build();

        when(dao.findById(id)).thenReturn(Optional.of(request));

        // Mock the getFile call to simulate a successful download
        when(basicFileAccessApi.getFile(any(HttpClientResponseHandler.class))).thenAnswer(invocation -> {
            HttpClientResponseHandler<?> handler = invocation.getArgument(0);
            ClassicHttpResponse response = mock(ClassicHttpResponse.class);
            HttpEntity entity = mock(HttpEntity.class);
            when(response.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(new ByteArrayInputStream("test data".getBytes()));
            return handler.handleResponse(response);
        });

        DownloadTask task = new DownloadTask(id, dao, dataverseClient, downloadConfig, quotaManager);
        task.run();

        assertThat(request.getStatus()).isEqualTo(TransferStatus.DOWNLOADED);
        assertThat(tempDir.resolve(id.toString())).exists().hasContent("test data");

        ArgumentCaptor<TransferRequest> captor = ArgumentCaptor.forClass(TransferRequest.class);
        verify(dao, org.mockito.Mockito.atLeastOnce()).save(captor.capture());
        assertThat(request.getStatus()).isEqualTo(TransferStatus.DOWNLOADED);
    }

    @Test
    void run_should_set_status_to_FAILED_on_exception() throws Exception {
        UUID id = UUID.randomUUID();
        TransferRequest request = TransferRequest.builder()
            .id(id)
            .dataverseFileId(123L)
            .status(TransferStatus.INSPECTED)
            .build();

        when(dao.findById(id)).thenReturn(Optional.of(request));
        when(basicFileAccessApi.getFile(any(HttpClientResponseHandler.class))).thenThrow(new RuntimeException("Download failed"));

        DownloadTask task = new DownloadTask(id, dao, dataverseClient, downloadConfig, quotaManager);
        try {
            task.run();
        } catch (RuntimeException e) {
            // expected
        }

        assertThat(request.getStatus()).isEqualTo(TransferStatus.FAILED);
        assertThat(request.getMessage()).contains("Download failed");
    }
}
