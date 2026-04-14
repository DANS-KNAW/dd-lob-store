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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lobstore.api.JobStatusDto;
import nl.knaw.dans.lobstore.db.JobDao;
import org.apache.commons.codec.digest.DigestUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.OffsetDateTime;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class DownloadTask {
    private final JobDao jobDao;
    private final Client httpClient;
    private final Path downloadFolder;
    private final long chunkSize;
    private final DiskQuotaManager diskQuotaManager;

    public void processJob(Job job) {
        try {
            log.info("Downloading job {}", job.getId());
            job.setStatus(JobStatusDto.DOWNLOADING);
            job.setModificationTimestamp(OffsetDateTime.now());
            jobDao.create(job);

            Path jobPath = downloadFolder.resolve(job.getId().toString());
            Files.createDirectories(jobPath);
            Path outputFile = jobPath.resolve("data");

            long totalRead = 0;
            try (Response response = httpClient.target(job.getUrl()).request().get()) {
                if (response.getStatus() != 200) {
                    throw new IOException("Failed to download: " + response.getStatus());
                }

                Long contentLength = response.getLength() != -1 ? (long) response.getLength() : null;
                if (contentLength != null && !diskQuotaManager.claim(job.getId(), "download", 2 * contentLength)) {
                    throw new IOException("Insufficient disk quota");
                }

                try (InputStream is = response.readEntity(InputStream.class);
                     OutputStream os = new BufferedOutputStream(Files.newOutputStream(outputFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))) {
                    byte[] buffer = new byte[8192];
                    int read;
                    while ((read = is.read(buffer)) != -1) {
                        os.write(buffer, 0, read);
                        totalRead += read;
                    }
                }
            }

            String sha1 = calculateSha1(outputFile);
            job.setSha1Sum(sha1);
            job.setFileSize(totalRead);
            job.setFilePath(outputFile.toString());
            job.setStatus(JobStatusDto.PACKAGING);
            job.setModificationTimestamp(OffsetDateTime.now());
            jobDao.create(job);

            diskQuotaManager.release(job.getId(), "download");
            diskQuotaManager.claim(job.getId(), "download", totalRead);

            log.info("Downloaded job {} to {}, SHA-1: {}", job.getId(), outputFile, sha1);
        } catch (Exception e) {
            log.error("Error processing job {}", job.getId(), e);
            job.setStatus(JobStatusDto.FAILED);
            job.setErrorMessage(e.getMessage());
            job.setModificationTimestamp(OffsetDateTime.now());
            jobDao.create(job);
        }
    }

    private String calculateSha1(Path file) throws IOException {
        try (InputStream is = Files.newInputStream(file)) {
            return DigestUtils.sha1Hex(is);
        }
    }
}
