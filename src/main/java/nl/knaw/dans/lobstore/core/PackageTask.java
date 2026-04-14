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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public class PackageTask implements Runnable {
    private final JobDao jobDao;
    private final Path uploadFolder;
    private final long minBucketSize;
    private final String dmftarCommand;
    private final DiskQuotaManager diskQuotaManager;

    @Override
    @UnitOfWork
    public void run() {
        List<Job> downloadedJobs = jobDao.findByStatus(JobStatusDto.PACKAGING);
        if (downloadedJobs.isEmpty()) {
            return;
        }

        Map<String, List<Job>> byDatastation = downloadedJobs.stream()
                .collect(Collectors.groupingBy(Job::getDatastation));

        for (Map.Entry<String, List<Job>> entry : byDatastation.entrySet()) {
            processDatastation(entry.getKey(), entry.getValue());
        }
    }

    private void processDatastation(String datastation, List<Job> jobs) {
        long totalSize = jobs.stream().mapToLong(Job::getFileSize).sum();
        if (totalSize < minBucketSize) {
            log.debug("Datastation {} bucket size {} below minBucketSize {}, waiting", datastation, totalSize, minBucketSize);
            return;
        }

        String bucketId = UUID.randomUUID().toString();
        log.info("Packaging bucket {} for datastation {} with {} jobs", bucketId, datastation, jobs.size());

        try {
            // Claim 2x the file sizes in the upload folder
            if (!diskQuotaManager.claim(UUID.fromString(bucketId), "upload", 2 * totalSize)) {
                 log.warn("Insufficient quota for upload folder for bucket {}", bucketId);
                 return;
            }

            Path bucketPath = uploadFolder.resolve(bucketId + ".tar");
            
            // Collect file paths
            List<String> filePaths = jobs.stream().map(Job::getFilePath).collect(Collectors.toList());
            
            // Execute dmftar
            executeDmftar(bucketPath, filePaths);

            OffsetDateTime now = OffsetDateTime.now();
            for (Job job : jobs) {
                job.setStatus(JobStatusDto.TRANSFERRING);
                job.setBucketId(bucketId);
                job.setModificationTimestamp(now);
                jobDao.create(job);
                
                // Release the claimed space in the download folder
                diskQuotaManager.release(job.getId(), "download");
            }
            
            // Release 1x the file sizes in the upload folder (so 1x remains)
            diskQuotaManager.release(UUID.fromString(bucketId), "upload");
            diskQuotaManager.claim(UUID.fromString(bucketId), "upload", totalSize);

            log.info("Packaged bucket {} to {}", bucketId, bucketPath);

        } catch (Exception e) {
            log.error("Failed to package bucket {} for datastation {}", bucketId, datastation, e);
            // Don't fail jobs here, let them be retried or handled later
        }
    }

    private void executeDmftar(Path bucketPath, List<String> filePaths) throws IOException, InterruptedException {
        // Simple implementation: dmftar -c bucketPath file1 file2 ...
        // In reality, this might need a more complex command or input file
        List<String> command = new java.util.ArrayList<>();
        command.add(dmftarCommand);
        command.add("-c");
        command.add(bucketPath.toString());
        command.addAll(filePaths);

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("dmftar failed with exit code " + exitCode);
        }
    }
}
