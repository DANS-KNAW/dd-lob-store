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
import nl.knaw.dans.lobstore.Conversions;
import nl.knaw.dans.lobstore.api.TransferRequestDto;
import nl.knaw.dans.lobstore.api.TransferResponseDto;
import nl.knaw.dans.lobstore.api.TransferResponseItemDto;
import nl.knaw.dans.lobstore.api.TransferStatusDto;
import nl.knaw.dans.lobstore.api.TransferStatusInfoDto;
import nl.knaw.dans.lobstore.core.BucketStatus;
import nl.knaw.dans.lobstore.core.TransferRequest;
import nl.knaw.dans.lobstore.core.TransferRequestStatus;
import nl.knaw.dans.lobstore.db.LocationDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.mapstruct.factory.Mappers;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class TransfersResource implements TransfersApi {
    private final Conversions conversions = Mappers.getMapper(Conversions.class);
    private final TransferRequestDao transferRequestDao;
    private final LocationDao locationDao;

    public TransfersResource(TransferRequestDao transferRequestDao, LocationDao locationDao) {
        this.transferRequestDao = transferRequestDao;
        this.locationDao = locationDao;
    }

    @Override
    @UnitOfWork
    public Response addTransfers(@Valid @NotNull List<@Valid TransferRequestDto> transferRequestDtoList) {
        List<TransferResponseItemDto> results = new ArrayList<>();

        for (TransferRequestDto dto : transferRequestDtoList) {
            results.add(processSingleTransfer(dto));
        }

        return Response.status(207) // Multi-Status
            .entity(results)
            .build();
    }

    private TransferResponseItemDto processSingleTransfer(TransferRequestDto transferRequestDto) {
        String sha1 = transferRequestDto.getSha1Sum();
        String datastation = transferRequestDto.getDatastation();
        Long fileId = transferRequestDto.getDataverseFileId();

        List<TransferRequest> existingRequestsForSha1 = transferRequestDao.findBySha1Sum(sha1);
        if (existingRequestsForSha1.stream()
            .filter(request -> request.getDatastation().equals(datastation))
            .anyMatch(TransferRequest::isInProgress)) {
            return new TransferResponseItemDto()
                .status(Response.Status.CONFLICT.getStatusCode())
                .message("Transfer already in progress for the given SHA-1");
        }

        List<TransferRequest> existingRequestsForFileId = transferRequestDao.findByDataverseFileId(fileId);
        if (existingRequestsForFileId.stream()
            .filter(request -> request.getDatastation().equals(datastation))
            .anyMatch(TransferRequest::isInProgress)) {
            return new TransferResponseItemDto()
                .status(Response.Status.CONFLICT.getStatusCode())
                .message("Transfer already in progress for the given Dataverse file ID");
        }

        var existingLocation = locationDao.findByDatastationAndSha1Sum(datastation, sha1);

        if (existingLocation.isPresent()) {
            return new TransferResponseItemDto()
                .status(Response.Status.SEE_OTHER.getStatusCode())
                .location(String.format("/locations/%s/%s", datastation, sha1));
        }

        TransferRequest newRequest = TransferRequest.builder()
            .id(UUID.randomUUID())
            .dataverseFileId(fileId)
            .sha1Sum(sha1)
            .datastation(datastation)
            .status(TransferRequestStatus.PENDING)
            .created(OffsetDateTime.now())
            .build();

        transferRequestDao.save(newRequest);

        return new TransferResponseItemDto()
            .id(newRequest.getId())
            .status(Response.Status.CREATED.getStatusCode());
    }

    @Override
    @UnitOfWork
    public Response getTransferStatus(@NotNull UUID id) {
        return transferRequestDao.findById(id)
            .map(conversions::convert)
            .map(statusInfo -> Response.ok(statusInfo).build())
            .orElseThrow(() -> new WebApplicationException("Transfer not found", Response.Status.NOT_FOUND));
    }
}
