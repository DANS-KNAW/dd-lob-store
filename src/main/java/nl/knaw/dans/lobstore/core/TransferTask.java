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
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public class TransferTask {
    private final JobDao jobDao;
    private final Path uploadFolder;
    private final String rsyncCommand;
    private final String transferDestination;

    public void processBucket(String bucketId, List<Job> jobs) {
        log.info("Transferring bucket {} for {} jobs", bucketId, jobs.size());
        Path bucketPath = uploadFolder.resolve(bucketId + ".tar");

        try {
            executeRsync(bucketPath);

            OffsetDateTime now = OffsetDateTime.now();
            for (Job job : jobs) {
                job.setStatus(JobStatusDto.VERIFYING);
                job.setModificationTimestamp(now);
                jobDao.create(job);
            }
            log.info("Transferred bucket {} to destination", bucketId);

        } catch (Exception e) {
            log.error("Failed to transfer bucket {}", bucketId, e);
        }
    }

    private void executeRsync(Path bucketPath) throws IOException {
        // rsync -a bucketPath destination/
        CommandLine commandLine = new CommandLine(rsyncCommand);
        commandLine.addArgument("-a");
        commandLine.addArgument(bucketPath.toString());
        commandLine.addArgument(transferDestination);

        DefaultExecutor executor = DefaultExecutor.builder().get();
        int exitCode = executor.execute(commandLine);
        if (exitCode != 0) {
            throw new IOException("rsync failed with exit code " + exitCode);
        }
    }
}
