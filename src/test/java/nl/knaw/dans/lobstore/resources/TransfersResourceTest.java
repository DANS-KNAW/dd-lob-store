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
import nl.knaw.dans.lobstore.api.TransferRequestDto;
import nl.knaw.dans.lobstore.api.TransferResponseDto;
import nl.knaw.dans.lobstore.core.Bucket;
import nl.knaw.dans.lobstore.core.BucketStatus;
import nl.knaw.dans.lobstore.core.Location;
import nl.knaw.dans.lobstore.core.TransferRequest;
import nl.knaw.dans.lobstore.core.TransferRequestStatus;
import nl.knaw.dans.lobstore.db.LocationDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(DropwizardExtensionsSupport.class)
class TransfersResourceTest {

    private static final TransferRequestDao dao = mock(TransferRequestDao.class);
    private static final LocationDao locationDao = mock(LocationDao.class);
    private static final ResourceExtension EXT = ResourceExtension.builder()
        .addResource(new TransfersResource(dao, locationDao))
        .build();

    @BeforeEach
    void setup() {
    }

    @AfterEach
    void tearDown() {
        reset(dao);
        reset(locationDao);
    }

    @Test
    void add_transfer_should_return_201_and_id_for_new_request() {
        TransferRequestDto dto = new TransferRequestDto()
            .dataverseFileId(123L)
            .sha1Sum("abc")
            .datastation("station1");

        when(dao.findBySha1Sum("abc")).thenReturn(Collections.emptyList());

        Response response = EXT.target("/transfers")
            .request(MediaType.APPLICATION_JSON)
            .post(Entity.entity(dto, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(201);
        TransferResponseDto result = response.readEntity(TransferResponseDto.class);
        assertThat(result.getId()).isNotNull();

        ArgumentCaptor<TransferRequest> captor = ArgumentCaptor.forClass(TransferRequest.class);
        verify(dao).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(TransferRequestStatus.PENDING);
        assertThat(captor.getValue().getSha1Sum()).isEqualTo("abc");
    }

    @Test
    void add_transfer_should_return_409_if_already_in_progress_for_same_datastation() {
        TransferRequestDto dto = new TransferRequestDto()
            .dataverseFileId(123L)
            .sha1Sum("abc")
            .datastation("station1");

        TransferRequest existing = TransferRequest.builder()
            .status(TransferRequestStatus.PENDING)
            .sha1Sum("abc")
            .datastation("station1")
            .build();

        when(dao.findBySha1Sum("abc")).thenReturn(List.of(existing));

        Response response = EXT.target("/transfers")
            .request(MediaType.APPLICATION_JSON)
            .post(Entity.entity(dto, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(409);
    }

    @Test
    void add_transfer_should_return_201_if_already_in_progress_for_different_datastation() {
        TransferRequestDto dto = new TransferRequestDto()
            .dataverseFileId(123L)
            .sha1Sum("abc")
            .datastation("station1");

        TransferRequest existing = TransferRequest.builder()
            .status(TransferRequestStatus.PENDING)
            .sha1Sum("abc")
            .datastation("station2")
            .build();

        when(dao.findBySha1Sum("abc")).thenReturn(List.of(existing));

        Response response = EXT.target("/transfers")
            .request(MediaType.APPLICATION_JSON)
            .post(Entity.entity(dto, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(201);
    }

    @Test
    void add_transfer_should_return_303_if_already_done_for_same_datastation() {
        TransferRequestDto dto = new TransferRequestDto()
            .dataverseFileId(123L)
            .sha1Sum("abc")
            .datastation("station1");

        Location existingLocation = Location.builder()
            .datastation("station1")
            .sha1Sum("abc")
            .bucketName("bucket1")
            .build();

        when(dao.findBySha1Sum("abc")).thenReturn(Collections.emptyList());
        when(locationDao.findByDatastationAndSha1Sum("station1", "abc")).thenReturn(java.util.Optional.of(existingLocation));

        Response response = EXT.target("/transfers")
            .property(ClientProperties.FOLLOW_REDIRECTS, false)
            .request(MediaType.APPLICATION_JSON)
            .post(Entity.entity(dto, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(303);
        assertThat(response.getHeaderString("Location")).endsWith("/locations/station1/abc");

        verify(dao, org.mockito.Mockito.never()).save(any());
    }

    @Test
    void add_transfer_should_return_201_if_already_done_for_different_datastation() {
        TransferRequestDto dto = new TransferRequestDto()
            .dataverseFileId(123L)
            .sha1Sum("abc")
            .datastation("station1");

        when(dao.findBySha1Sum("abc")).thenReturn(Collections.emptyList());
        // Location for different datastation should NOT trigger 303 for THIS request
        when(locationDao.findByDatastationAndSha1Sum("station1", "abc")).thenReturn(java.util.Optional.empty());

        Response response = EXT.target("/transfers")
            .request(MediaType.APPLICATION_JSON)
            .post(Entity.entity(dto, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(201);
    }
}
