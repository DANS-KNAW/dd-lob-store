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
package nl.knaw.dans.lobstore.resources;

import io.dropwizard.hibernate.UnitOfWork;
import nl.knaw.dans.lobstore.api.JobRequestDto;
import nl.knaw.dans.lobstore.api.JobStatusInfoDto;
import nl.knaw.dans.lobstore.db.JobDao;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.UUID;

public class JobsResource implements JobsApi {
    private final JobDao jobDao;

    public JobsResource(JobDao jobDao) {
        this.jobDao = jobDao;
    }

    @Override
    @UnitOfWork
    public Response addJob(JobRequestDto jobRequestDto) {
//        Job job = Job.builder()
//                .id(UUID.randomUUID())
//
//                .datastation(jobRequestDto.getDatastation())
//                .status(JobStatusDto.PENDING)
//                .creationTimestamp(OffsetDateTime.now())
//                .modificationTimestamp(OffsetDateTime.now())
//                .build();
//
//        Job saved = jobDao.create(job);
//        JobResponseDto response = new JobResponseDto();
//        response.setId(saved.getId());
//        return Response.status(Response.Status.CREATED).entity(response).build();
        return null;
    }

    @Override
    @UnitOfWork
    public Response getJobStatus(UUID id) {
        return jobDao.findById(id)
                .map(job -> {
                    JobStatusInfoDto info = new JobStatusInfoDto();
                    info.setId(job.getId());
                    info.setStatus(job.getStatus());
                    return Response.ok(info).build();
                })
                .orElseThrow(() -> new NotFoundException("Job not found: " + id));
    }
}
