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

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import nl.knaw.dans.lobstore.api.LocationResponseDto;
import nl.knaw.dans.lobstore.core.Location;
import nl.knaw.dans.lobstore.db.LocationDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.core.Response;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@ExtendWith(DropwizardExtensionsSupport.class)
class LocationsResourceTest {

    private static final LocationDao dao = mock(LocationDao.class);
    private static final ResourceExtension EXT = ResourceExtension.builder()
        .addResource(new LocationsResource(dao))
        .build();

    @AfterEach
    void tearDown() {
        reset(dao);
    }

    @Test
    void getLocationByHash_should_return_200_if_found() {
        String store = "station1";
        String hash = "sha1";
        Location location = Location.builder()
            .datastation(store)
            .sha1Sum(hash)
            .bucketName("bucket1")
            .build();

        when(dao.findByDatastationAndSha1Sum(store, hash)).thenReturn(Optional.of(location));

        Response response = EXT.target("/locations/" + store + "/" + hash).request().get();

        assertThat(response.getStatus()).isEqualTo(200);
        LocationResponseDto result = response.readEntity(LocationResponseDto.class);
        assertThat(result.getDatastation()).isEqualTo(store);
        assertThat(result.getBucket()).isEqualTo("bucket1");
    }

    @Test
    void getLocationByHash_should_return_404_if_not_found() {
        String store = "station1";
        String hash = "sha1";

        when(dao.findByDatastationAndSha1Sum(store, hash)).thenReturn(Optional.empty());

        Response response = EXT.target("/locations/" + store + "/" + hash).request().get();

        assertThat(response.getStatus()).isEqualTo(404);
    }
}
