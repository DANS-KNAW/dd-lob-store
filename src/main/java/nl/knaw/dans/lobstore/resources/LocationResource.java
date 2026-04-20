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
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lobstore.api.LocationResponseDto;
import nl.knaw.dans.lobstore.db.LocationDao;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;

@RequiredArgsConstructor
public class LocationResource implements LocationApi {
    private final LocationDao locationDao;

    @Override
    @UnitOfWork
    public Response getLocationByHash(@NotNull String store, @NotNull String hash) {
        return locationDao.findByDatastationAndSha1Sum(store, hash)
            .map(location -> Response.ok(new LocationResponseDto(location.getDatastation(), location.getBucketName())).build())
            .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
    }
}
