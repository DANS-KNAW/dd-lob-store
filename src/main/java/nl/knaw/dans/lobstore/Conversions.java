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
package nl.knaw.dans.lobstore;

import nl.knaw.dans.lobstore.api.TransferStatusDto;
import nl.knaw.dans.lobstore.api.TransferStatusInfoDto;
import nl.knaw.dans.lobstore.core.TransferRequest;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper
public interface Conversions {

    @Mapping(target = "status", expression = "java(mapStatus(transferRequest))")
    TransferStatusInfoDto convert(TransferRequest transferRequest);

    default TransferStatusDto mapStatus(TransferRequest tr) {
        if (tr.getBucket() != null) {
            return switch (tr.getBucket().getStatus()) {
                case OPEN -> TransferStatusDto.DOWNLOADED;
                case PACKAGING -> TransferStatusDto.PACKAGING;
                case PACKAGED -> TransferStatusDto.PACKAGED;
                case UPLOADING -> TransferStatusDto.UPLOADING;
                case UPLOADED -> TransferStatusDto.UPLOADED;
                case VERIFYING -> TransferStatusDto.VERIFYING;
                case DONE -> TransferStatusDto.DONE;
                case FAILED -> TransferStatusDto.FAILED;
            };
        }

        return switch (tr.getStatus()) {
            case PENDING -> TransferStatusDto.PENDING;
            case INSPECTED -> TransferStatusDto.INSPECTED;
            case DOWNLOADING -> TransferStatusDto.DOWNLOADING;
            case DOWNLOADED -> TransferStatusDto.DOWNLOADED;
            case REJECTED -> TransferStatusDto.REJECTED;
            case FAILED -> TransferStatusDto.FAILED;
        };
    }
}
