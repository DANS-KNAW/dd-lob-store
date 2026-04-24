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
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class UploadTask implements Runnable {
    private final UUID bucketId;
    private final BucketDao bucketDao;
    private final ExternalCommandConfig uploadCommand;
    private final Map<String, DataStationConfig> datastations;
    private final ActiveTaskRegistry activeTaskRegistry;

    @Override
    @UnitOfWork
    public void run() {
        try {
            log.info("Starting UPLOAD task for bucket {}", bucketId);
            Bucket bucket = bucketDao.findById(bucketId).orElseThrow(() -> new RuntimeException("Bucket not found: " + bucketId));

            String datastationName = bucket.getDatastation();
            DataStationConfig dsConfig = datastations.get(datastationName);
            if (dsConfig == null) {
                throw new IllegalStateException("DataStation configuration not found for: " + datastationName);
            }

            executeUploadCommand(bucketId.toString(), datastationName, dsConfig);

            bucket.setStatus(BucketStatus.UPLOADED);
            bucketDao.save(bucket);
            log.info("Successfully finished UPLOAD task for bucket {}", bucketId);
        }
        catch (Exception e) {
            log.error("Error during upload for bucket {}", bucketId, e);
            // Leaves the bucket in UPLOADING state, so it will be retried next polling round.
        }
        finally {
            activeTaskRegistry.remove(bucketId);
        }
    }

    private void executeUploadCommand(String bucketName, String datastationName, DataStationConfig dsConfig) throws IOException {
        CommandLine commandLine = new CommandLine(uploadCommand.getExecutable());
        for (String arg : uploadCommand.getArgs()) {
            commandLine.addArgument(interpolate(arg, bucketName, datastationName, dsConfig));
        }

        log.debug("Executing upload command: {}", commandLine);

        var builder = new DefaultExecutor.Builder<>();
        if (uploadCommand.getWorkingDirectory() != null) {
            String wd = interpolate(uploadCommand.getWorkingDirectory(), bucketName, datastationName, dsConfig);
            builder.setWorkingDirectory(new File(wd));
        }
        var executor = builder.get();

        executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));

        try {
            int exitCode = executor.execute(commandLine);
            if (exitCode != 0) {
                throw new RuntimeException("Upload command failed with exit code " + exitCode);
            }
        }
        catch (ExecuteException e) {
            throw new RuntimeException("Upload command failed", e);
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
