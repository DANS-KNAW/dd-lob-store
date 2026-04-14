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
import nl.knaw.dans.lobstore.api.JobStatusDto;
import nl.knaw.dans.lobstore.resources.LocationApi;
import nl.knaw.dans.lobstore.api.LocationResponseDto;
import nl.knaw.dans.lobstore.db.JobDao;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

public class LocationResource implements LocationApi {
    private final JobDao jobDao;

    public LocationResource(JobDao jobDao) {
        this.jobDao = jobDao;
    }

    @Override
    @UnitOfWork
    public Response getLocationByHash(String hash) {
        return jobDao.findBySha1SumAndStatus(hash, JobStatusDto.DONE)
                .map(job -> {
                    LocationResponseDto dto = new LocationResponseDto();
                    dto.setLocation(job.getBucketId()); // Or some other way to represent location
                    return Response.ok(dto).build();
                })
                .orElseThrow(() -> new NotFoundException("Location not found for SHA-1: " + hash));
    }
}
