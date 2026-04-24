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
import nl.knaw.dans.lobstore.config.DataStationConfig;
import nl.knaw.dans.lobstore.config.ExternalCommandConfig;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.LocationDao;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class VerifyTask implements Runnable {
    private static final Set<PosixFilePermission> deletePermissions = PosixFilePermissions.fromString("rwxr-xr-x");

    private final UUID bucketId;
    private final BucketDao bucketDao;
    private final LocationDao locationDao;
    private final ExternalCommandConfig verifyCommand;
    private final String invalidOn;
    private final Map<String, DataStationConfig> datastations;
    private final Path uploadDir;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;

    @Override
    @UnitOfWork
    public void run() {
        try {
            log.info("Starting VERIFY task for bucket {}", bucketId);
            Bucket bucket = bucketDao.findById(bucketId).orElseThrow(() -> new RuntimeException("Bucket not found: " + bucketId));

            String datastationName = bucket.getDatastation();
            DataStationConfig dsConfig = datastations.get(datastationName);
            if (dsConfig == null) {
                throw new IllegalStateException("DataStation configuration not found for: " + datastationName);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            int exitCode = executeVerifyCommand(bucketId.toString(), datastationName, dsConfig, outputStream);

            if (exitCode == 0) {
                for (var tr : bucket.getTransferRequests()) {
                    locationDao.save(Location.builder()
                        .datastation(datastationName)
                        .sha1Sum(tr.getSha1Sum())
                        .bucketName(bucketId.toString())
                        .build());
                }

                // If the command exits with success the local bucket should be removed
                Path bucketFile = uploadDir.resolve(bucketId.toString() + ".dmftar");

                // dmftar makes everything readonly, even the directories, so we need to set write-permissions to be able to delete the files in them.
                try (var stream = Files.walk(bucketFile)) {
                    stream.filter(Files::isDirectory).forEach(path -> {
                        try {
                            Files.setPosixFilePermissions(path, deletePermissions);
                        }
                        catch (IOException e) {
                            log.warn("Failed to set delete permissions for {}: {}", path, e.getMessage());
                        }
                    });
                }

                if (Files.exists(bucketFile)) {
                    log.info("Removing local bucket file: {}", bucketFile);
                    FileUtils.deleteDirectory(bucketFile.toFile());
                }

                // and the remaining claim with extension /base should be released.
                for (var tr : bucket.getTransferRequests()) {
                    quotaManager.release(tr.getId() + "/base", "download");
                }

                bucket.setStatus(BucketStatus.DONE);
                bucketDao.save(bucket);
                log.info("Successfully finished VERIFY task for bucket {}", bucketId);
            }
            else {
                String stderr = outputStream.toString(Charset.defaultCharset());
                if (invalidOn != null && stderr.contains(invalidOn)) {
                    log.error("Verify command indicated invalid dmftar for bucket {}: {}", bucketId, stderr);
                    bucket.setStatus(BucketStatus.FAILED);
                    bucketDao.save(bucket);
                }
                else {
                    log.warn("Verify command failed with exit code {} for bucket {} but 'invalidOn' string not found in stderr. Stderr: {}", exitCode, bucketId, stderr);
                    // Do nothing, leave in current state for retry
                }
            }
        }
        catch (Exception e) {
            log.error("Error during verify for bucket {}", bucketId, e);
            // If interrupted the task should not remove anything but just leave the bucket in verifying state,
            // so that a next try will execute the same command.
        }
        finally {
            activeTaskRegistry.remove(bucketId);
        }
    }

    private int executeVerifyCommand(String bucketName, String datastationName, DataStationConfig dsConfig, ByteArrayOutputStream outputStream) throws IOException {
        CommandLine commandLine = new CommandLine(verifyCommand.getExecutable());
        for (String arg : verifyCommand.getArgs()) {
            commandLine.addArgument(interpolate(arg, bucketName, datastationName, dsConfig));
        }

        log.debug("Executing verify command: {}", commandLine);

        var builder = new DefaultExecutor.Builder<>();
        if (verifyCommand.getWorkingDirectory() != null) {
            String wd = interpolate(verifyCommand.getWorkingDirectory(), bucketName, datastationName, dsConfig);
            builder.setWorkingDirectory(new File(wd));
        }
        var executor = builder.get();

        executor.setStreamHandler(new PumpStreamHandler(outputStream, outputStream));

        try {
            return executor.execute(commandLine);
        }
        catch (ExecuteException e) {
            return e.getExitValue();
        }
    }

    private String interpolate(String text, String bucketName, String datastationName, DataStationConfig dsConfig) {
        return text.replace("${bucketname}", bucketName)
            .replace("${datastation}", datastationName)
            .replace("${user}", dsConfig.getLobstore().getUser())
            .replace("${host}", dsConfig.getLobstore().getHost())
            .replace("${path}", dsConfig.getLobstore().getPath().toString());
    }
}
