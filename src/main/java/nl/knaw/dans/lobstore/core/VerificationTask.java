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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@AllArgsConstructor
public class VerificationTask implements Runnable {
    private final JobDao jobDao;
    private final Path downloadFolder;
    private final Path uploadFolder;
    private final String sshCommand;
    private final String verificationCommand;
    private final String transferDestination;
    private final DiskQuotaManager diskQuotaManager;

    @Override
    @UnitOfWork
    public void run() {
        List<Job> transferredJobs = jobDao.findByStatus(JobStatusDto.VERIFYING);
        if (transferredJobs.isEmpty()) {
            return;
        }

        Map<String, List<Job>> byBucket = transferredJobs.stream()
                .collect(Collectors.groupingBy(Job::getBucketId));

        for (Map.Entry<String, List<Job>> entry : byBucket.entrySet()) {
            processBucket(entry.getKey(), entry.getValue());
        }
    }

    private void processBucket(String bucketId, List<Job> jobs) {
        log.info("Verifying bucket {} for {} jobs", bucketId, jobs.size());

        try {
            executeVerification(bucketId);

            OffsetDateTime now = OffsetDateTime.now();
            for (Job job : jobs) {
                job.setStatus(JobStatusDto.DONE);
                job.setModificationTimestamp(now);
                jobDao.create(job);
                
                // Cleanup downloaded file
                cleanupJobFile(job);
            }
            
            // Cleanup upload bucket
            cleanupBucketFile(bucketId);
            
            // Release quota
            diskQuotaManager.release(UUID.fromString(bucketId));
            for (Job job : jobs) {
                diskQuotaManager.release(job.getId());
            }

            log.info("Verified and cleaned up bucket {}", bucketId);

        } catch (Exception e) {
            log.error("Failed to verify bucket {}", bucketId, e);
        }
    }

    private void executeVerification(String bucketId) throws IOException {
        // ssh remoteHost 'dmftar --verify /path/to/dest/bucketId.tar'
        // destination is user@host:/path/to/dest
        String[] parts = transferDestination.split(":");
        String host = parts[0];
        String remoteDir = parts[1];
        String remotePath = remoteDir + "/" + bucketId + ".tar";

        CommandLine commandLine = new CommandLine(sshCommand);
        commandLine.addArgument(host);
        commandLine.addArgument(verificationCommand + " " + remotePath);

        DefaultExecutor executor = DefaultExecutor.builder().get();
        int exitCode = executor.execute(commandLine);
        if (exitCode != 0) {
            throw new IOException("Verification failed with exit code " + exitCode);
        }
    }

    private void cleanupJobFile(Job job) {
        try {
            Path jobPath = downloadFolder.resolve(job.getId().toString());
            // Recursive delete
            deleteRecursively(jobPath);
        } catch (IOException e) {
            log.warn("Failed to cleanup downloaded file for job {}", job.getId(), e);
        }
    }

    private void cleanupBucketFile(String bucketId) {
        try {
            Path bucketPath = uploadFolder.resolve(bucketId + ".tar");
            Files.deleteIfExists(bucketPath);
        } catch (IOException e) {
            log.warn("Failed to cleanup upload bucket {}", bucketId, e);
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (Stream<Path> entries = Files.list(path)) {
                for (Path entry : entries.collect(Collectors.toList())) {
                    deleteRecursively(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
