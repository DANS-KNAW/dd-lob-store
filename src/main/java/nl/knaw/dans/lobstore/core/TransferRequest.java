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
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "transfer_request")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferRequest {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "dataverse_file_id", nullable = false)
    private Long dataverseFileId;

    @Column(name = "sha1_sum", nullable = false)
    private String sha1Sum;

    @Column(name = "datastation", nullable = false)
    private String datastation;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private TransferStatus status;

    @Column(name = "file_size")
    private Long fileSize;

    public boolean isInProgress() {
        return switch (status) {
            case PENDING, INSPECTING, INSPECTED, DOWNLOADING, DOWNLOADED, PACKAGING, PACKAGED, TRANSFERRING, TRANSFERRED, VERIFYING -> true;
            case DONE, REJECTED, FAILED -> false;
        };
    }
}
