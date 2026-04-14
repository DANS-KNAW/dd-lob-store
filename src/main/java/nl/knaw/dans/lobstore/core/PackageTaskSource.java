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

import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskSource;
import nl.knaw.dans.lobstore.api.JobStatusDto;
import nl.knaw.dans.lobstore.db.JobDao;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class PackageTaskSource implements TaskSource<Job> {
    private final JobDao jobDao;

    @Override
    public List<Job> nextInputs() {
        List<Job> downloadedJobs = jobDao.findByStatus(JobStatusDto.PACKAGING);
        if (downloadedJobs.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, List<Job>> byDatastation = downloadedJobs.stream()
                .collect(Collectors.groupingBy(Job::getDatastation));

        // Return first datastation's jobs
        return byDatastation.values().iterator().next();
    }
}
