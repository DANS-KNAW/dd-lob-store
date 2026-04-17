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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lobstore.db.JobDao;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class PackageTask {
    private final JobDao jobDao;
    private final Path uploadFolder;
    private final long minBucketSize;
    private final String dmftarCommand;
    private final DiskQuotaManager diskQuotaManager;

    public void processDatastation(String datastation, List<FileDownloadRequest> fileDownloadRequests) {

    }

    private void executeDmftar(Path bucketPath, List<String> filePaths) throws IOException {
        // Simple implementation: dmftar -c bucketPath file1 file2 ...
        // In reality, this might need a more complex command or input file
        CommandLine commandLine = new CommandLine(dmftarCommand);
        commandLine.addArgument("-c");
        commandLine.addArgument(bucketPath.toString());
        for (String filePath : filePaths) {
            commandLine.addArgument(filePath);
        }

        DefaultExecutor executor = DefaultExecutor.builder().get();
        int exitCode = executor.execute(commandLine);
        if (exitCode != 0) {
            throw new IOException("dmftar failed with exit code " + exitCode);
        }
    }
}
