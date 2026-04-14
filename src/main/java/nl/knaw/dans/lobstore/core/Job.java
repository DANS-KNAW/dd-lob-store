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
import nl.knaw.dans.lobstore.api.JobStatusDto;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "job")
@NamedQueries({
        @NamedQuery(
                name = "nl.knaw.dans.lobstore.core.Job.findByStatus",
                query = "SELECT j FROM Job j WHERE j.status = :status ORDER BY j.creationTimestamp ASC"
        ),
        @NamedQuery(
                name = "nl.knaw.dans.lobstore.core.Job.findBySha1SumAndStatus",
                query = "SELECT j FROM Job j WHERE j.sha1Sum = :sha1Sum AND j.status = :status"
        )
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Job {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "url", nullable = false)
    private String url;

    @Column(name = "sha1_sum")
    private String sha1Sum;

    @Column(name = "datastation", nullable = false)
    private String datastation;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private JobStatusDto status;

    @Column(name = "creation_timestamp", nullable = false)
    private OffsetDateTime creationTimestamp;

    @Column(name = "modification_timestamp", nullable = false)
    private OffsetDateTime modificationTimestamp;

    @Column(name = "file_path")
    private String filePath;

    @Column(name = "file_size")
    private Long fileSize;

    @Column(name = "bucket_id")
    private String bucketId;

    @Column(name = "error_message")
    private String errorMessage;
}
