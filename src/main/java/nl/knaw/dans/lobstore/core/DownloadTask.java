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
import java.util.UUID;

@Slf4j
@AllArgsConstructor
public class DownloadTask implements Runnable {
    private final UUID jobId;
    private final JobDao jobDao;
    private final Client httpClient;
    private final Path downloadFolder;
    private final long chunkSize;
    private final DiskQuotaManager diskQuotaManager;

    @UnitOfWork
    public void run() {

    }

    private String calculateSha1(Path file) throws IOException {
        try (InputStream is = Files.newInputStream(file)) {
            return DigestUtils.sha1Hex(is);
        }
    }
}
