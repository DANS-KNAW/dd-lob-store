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
import nl.knaw.dans.lobstore.api.TransferRequestDto;
import nl.knaw.dans.lobstore.api.TransferResponseDto;
import nl.knaw.dans.lobstore.api.TransferStatusDto;
import nl.knaw.dans.lobstore.api.TransferStatusInfoDto;
import nl.knaw.dans.lobstore.core.TransferRequest;
import nl.knaw.dans.lobstore.core.TransferStatus;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class TransfersResource implements TransfersApi {

    private final TransferRequestDao transferRequestDao;

    public TransfersResource(TransferRequestDao transferRequestDao) {
        this.transferRequestDao = transferRequestDao;
    }

    @Override
    @UnitOfWork
    public Response addTransfer(@Valid @NotNull TransferRequestDto transferRequestDto) {
        String sha1 = transferRequestDto.getSha1Sum();

        List<TransferRequest> existingRequestsForSha1 = transferRequestDao.findBySha1Sum(sha1);
        if (existingRequestsForSha1.stream()
            .anyMatch(TransferRequest::isInProgress)) {
            return Response.status(Response.Status.CONFLICT)
                .entity("Transfer(s) already in progress for the given SHA-1")
                .build();
        }

        List<TransferRequest> existingRequestsForFileId = transferRequestDao.findByDataverseFileId(transferRequestDto.getDataverseFileId());
        if (existingRequestsForFileId.stream()
            .anyMatch(TransferRequest::isInProgress)) {
            return Response.status(Response.Status.CONFLICT)
                .entity("Transfer(s) already in progress for the given Dataverse file ID")
                .build();
        }

        boolean alreadyDone = existingRequestsForSha1.stream()
            .anyMatch(r -> r.getStatus() == TransferStatus.DONE);

        TransferRequest newRequest = TransferRequest.builder()
            .id(UUID.randomUUID())
            .dataverseFileId(transferRequestDto.getDataverseFileId())
            .sha1Sum(sha1)
            .datastation(transferRequestDto.getDatastation())
            .status(alreadyDone ? TransferStatus.DONE : TransferStatus.PENDING)
            .created(OffsetDateTime.now())
            .build();

        transferRequestDao.save(newRequest);

        return Response.status(Response.Status.CREATED)
            .entity(new TransferResponseDto().id(newRequest.getId()))
            .build();
    }

    @Override
    @UnitOfWork
    public Response getTransferStatus(@NotNull UUID id) {
        return transferRequestDao.findById(id)
            .map(r -> Response.ok(new TransferStatusInfoDto()
                .id(r.getId())
                .status(TransferStatusDto.fromValue(r.getStatus().name()))).build())
            .orElseThrow(() -> new WebApplicationException("Transfer not found", Response.Status.NOT_FOUND));
    }
}
