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
import nl.knaw.dans.lib.dataverse.GetFileRange;
import nl.knaw.dans.lobstore.config.DownloadConfig;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.apache.commons.codec.digest.DigestUtils;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        downloadConfig.setDownloadDirectory(tempDir);
        when(dataverseClient.basicFileAccess(anyLong())).thenReturn(basicFileAccessApi);
    }

    @Test
    void run_should_download_small_file_and_verify_sha1() throws Exception {
        UUID id = UUID.randomUUID();
        String content = "test data";
        String sha1 = DigestUtils.sha1Hex(content);
        TransferRequest request = TransferRequest.builder()
            .id(id)
            .dataverseFileId(123L)
            .status(TransferStatus.INSPECTED)
            .fileSize((long) content.length())
            .sha1Sum(sha1)
            .build();

        when(dao.findById(id)).thenReturn(Optional.of(request));

        when(basicFileAccessApi.getFile(any(HttpClientResponseHandler.class))).thenAnswer(invocation -> {
            HttpClientResponseHandler<?> handler = invocation.getArgument(0);
            ClassicHttpResponse response = mock(ClassicHttpResponse.class);
            HttpEntity entity = mock(HttpEntity.class);
            when(response.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(new ByteArrayInputStream(content.getBytes()));
            return handler.handleResponse(response);
        });

        DownloadTask task = new DownloadTask(id, dao, dataverseClient, downloadConfig, quotaManager, executorService);
        task.run();

        verify(dao, org.mockito.Mockito.atLeast(2)).save(request);
        assertThat(request.getStatus()).isEqualTo(TransferStatus.DOWNLOADED);
        Path outputFile = tempDir.resolve(id.toString()).resolve(sha1);
        assertThat(outputFile).exists().hasContent(content);
    }

    @Test
    void run_should_download_large_file_in_chunks_and_merge() throws Exception {
        UUID id = UUID.randomUUID();
        // 3 chunks of 3 bytes each
        String content = "123456789";
        String sha1 = DigestUtils.sha1Hex(content);
        downloadConfig.setChunkSize(io.dropwizard.util.DataSize.bytes(3));

        TransferRequest request = TransferRequest.builder()
            .id(id)
            .dataverseFileId(123L)
            .status(TransferStatus.INSPECTED)
            .fileSize((long) content.length())
            .sha1Sum(sha1)
            .build();

        when(dao.findById(id)).thenReturn(Optional.of(request));

        when(basicFileAccessApi.getFile(any(GetFileRange.class), any(HttpClientResponseHandler.class))).thenAnswer(invocation -> {
            GetFileRange range = invocation.getArgument(0);
            HttpClientResponseHandler<?> handler = invocation.getArgument(1);
            String chunkContent = content.substring((int) range.getStart(), (int) range.getEnd() + 1);

            ClassicHttpResponse response = mock(ClassicHttpResponse.class);
            HttpEntity entity = mock(HttpEntity.class);
            when(response.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(new ByteArrayInputStream(chunkContent.getBytes()));
            return handler.handleResponse(response);
        });

        DownloadTask task = new DownloadTask(id, dao, dataverseClient, downloadConfig, quotaManager, executorService);
        task.run();

        verify(dao, org.mockito.Mockito.atLeast(2)).save(request);
        assertThat(request.getStatus()).isEqualTo(TransferStatus.DOWNLOADED);
        Path downloadDir = tempDir.resolve(id.toString());
        assertThat(downloadDir.resolve(sha1)).exists().hasContent(content);
        assertThat(downloadDir.resolve(sha1 + ".0")).doesNotExist();
        assertThat(downloadDir.resolve(sha1 + ".1")).doesNotExist();
        assertThat(downloadDir.resolve(sha1 + ".2")).doesNotExist();
    }

    @Test
    void run_should_resume_download_from_missing_chunks() throws Exception {
        UUID id = UUID.randomUUID();
        String content = "123456789";
        String sha1 = DigestUtils.sha1Hex(content);
        downloadConfig.setChunkSize(io.dropwizard.util.DataSize.bytes(3));

        Path downloadDir = tempDir.resolve(id.toString());
        java.nio.file.Files.createDirectories(downloadDir);
        // Pre-create chunk 0 and 2
        java.nio.file.Files.writeString(downloadDir.resolve(sha1 + ".0"), "123");
        java.nio.file.Files.writeString(downloadDir.resolve(sha1 + ".2"), "789");

        TransferRequest request = TransferRequest.builder()
            .id(id)
            .dataverseFileId(123L)
            .status(TransferStatus.INSPECTED)
            .fileSize((long) content.length())
            .sha1Sum(sha1)
            .build();

        when(dao.findById(id)).thenReturn(Optional.of(request));

        // Only chunk 1 should be downloaded
        when(basicFileAccessApi.getFile(any(GetFileRange.class), any(HttpClientResponseHandler.class))).thenAnswer(invocation -> {
            GetFileRange range = invocation.getArgument(0);
            assertThat(range.getStart()).isEqualTo(3);
            assertThat(range.getEnd()).isEqualTo(5);

            HttpClientResponseHandler<?> handler = invocation.getArgument(1);
            ClassicHttpResponse response = mock(ClassicHttpResponse.class);
            HttpEntity entity = mock(HttpEntity.class);
            when(response.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(new ByteArrayInputStream("456".getBytes()));
            return handler.handleResponse(response);
        });

        DownloadTask task = new DownloadTask(id, dao, dataverseClient, downloadConfig, quotaManager, executorService);
        task.run();

        verify(dao, org.mockito.Mockito.atLeast(2)).save(request);
        assertThat(request.getStatus()).isEqualTo(TransferStatus.DOWNLOADED);
        assertThat(downloadDir.resolve(sha1)).exists().hasContent(content);
    }

    @Test
    void run_should_fail_if_sha1_mismatch() throws Exception {
        UUID id = UUID.randomUUID();
        String content = "test data";
        String sha1 = DigestUtils.sha1Hex(content);
        String wrongSha1 = DigestUtils.sha1Hex("wrong data");

        TransferRequest request = TransferRequest.builder()
            .id(id)
            .dataverseFileId(123L)
            .status(TransferStatus.INSPECTED)
            .fileSize((long) content.length())
            .sha1Sum(wrongSha1)
            .build();

        when(dao.findById(id)).thenReturn(Optional.of(request));

        when(basicFileAccessApi.getFile(any(HttpClientResponseHandler.class))).thenAnswer(invocation -> {
            HttpClientResponseHandler<?> handler = invocation.getArgument(0);
            ClassicHttpResponse response = mock(ClassicHttpResponse.class);
            HttpEntity entity = mock(HttpEntity.class);
            when(response.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(new ByteArrayInputStream(content.getBytes()));
            return handler.handleResponse(response);
        });

        DownloadTask task = new DownloadTask(id, dao, dataverseClient, downloadConfig, quotaManager, executorService);
        try {
            task.run();
        } catch (RuntimeException e) {
            // expected
        }

        assertThat(request.getStatus()).isEqualTo(TransferStatus.FAILED);
        assertThat(request.getMessage()).contains("SHA-1 mismatch");
    }

    @Test
    void run_should_set_status_to_FAILED_on_exception() throws Exception {
        UUID id = UUID.randomUUID();
        TransferRequest request = TransferRequest.builder()
            .id(id)
            .dataverseFileId(123L)
            .status(TransferStatus.INSPECTED)
            .fileSize(9L)
            .sha1Sum("some-sha1")
            .build();

        when(dao.findById(id)).thenReturn(Optional.of(request));
        when(basicFileAccessApi.getFile(any(HttpClientResponseHandler.class))).thenThrow(new RuntimeException("Download failed"));

        DownloadTask task = new DownloadTask(id, dao, dataverseClient, downloadConfig, quotaManager, executorService);
        try {
            task.run();
        } catch (RuntimeException e) {
            // expected
        }

        assertThat(request.getStatus()).isEqualTo(TransferStatus.FAILED);
        assertThat(request.getMessage()).contains("Download failed");
    }
}
