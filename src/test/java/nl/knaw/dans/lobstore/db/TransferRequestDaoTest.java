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
package nl.knaw.dans.lobstore.db;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.lobstore.core.TransferRequest;
import nl.knaw.dans.lobstore.core.TransferStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
class TransferRequestDaoTest {

    private final DAOTestExtension db = DAOTestExtension.newBuilder()
        .addEntityClass(TransferRequest.class)
        .build();

    private TransferRequestDao dao;

    @BeforeEach
    void setUp() {
        dao = new TransferRequestDao(db.getSessionFactory());
    }

    @Test
    void findByDataverseFileId_should_return_matching_requests() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        long fileId = 123L;

        db.inTransaction(() -> {
            dao.save(TransferRequest.builder()
                .id(id1)
                .dataverseFileId(fileId)
                .sha1Sum("sha1")
                .datastation("station1")
                .status(TransferStatus.PENDING)
                .created(OffsetDateTime.now())
                .build());

            dao.save(TransferRequest.builder()
                .id(id2)
                .dataverseFileId(fileId)
                .sha1Sum("sha2")
                .datastation("station1")
                .status(TransferStatus.DONE)
                .created(OffsetDateTime.now())
                .build());

            dao.save(TransferRequest.builder()
                .id(UUID.randomUUID())
                .dataverseFileId(456L)
                .sha1Sum("sha3")
                .datastation("station1")
                .status(TransferStatus.PENDING)
                .created(OffsetDateTime.now())
                .build());
        });

        List<TransferRequest> result = dao.findByDataverseFileId(fileId);
        assertThat(result).hasSize(2);
        assertThat(result).extracting(TransferRequest::getId).containsExactlyInAnyOrder(id1, id2);
    }

    @Test
    void findByDataverseFileId_should_return_empty_list_if_no_match() {
        List<TransferRequest> result = dao.findByDataverseFileId(999L);
        assertThat(result).isEmpty();
    }

    @Test
    void findInspectableItems_should_return_pending_requests_sorted_by_created_asc() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        db.inTransaction(() -> {
            dao.save(TransferRequest.builder()
                .id(id1)
                .dataverseFileId(1L)
                .sha1Sum("sha1")
                .datastation("station1")
                .status(TransferStatus.PENDING)
                .created(now.minusMinutes(10))
                .build());

            dao.save(TransferRequest.builder()
                .id(id2)
                .dataverseFileId(2L)
                .sha1Sum("sha2")
                .datastation("station1")
                .status(TransferStatus.PENDING)
                .created(now.minusMinutes(5))
                .build());

            dao.save(TransferRequest.builder()
                .id(id3)
                .dataverseFileId(3L)
                .sha1Sum("sha3")
                .datastation("station1")
                .status(TransferStatus.DONE)
                .created(now.minusMinutes(15))
                .build());
        });

        List<TransferRequest> result = dao.findInspectableItems();
        assertThat(result).hasSize(2);
        assertThat(result).extracting(TransferRequest::getId).containsExactly(id1, id2);
    }
    @Test
    void findNextDownloadableItem_should_return_inspected_request() {
        UUID id1 = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        db.inTransaction(() -> {
            dao.save(TransferRequest.builder()
                .id(id1)
                .dataverseFileId(1L)
                .sha1Sum("sha1")
                .datastation("station1")
                .status(TransferStatus.INSPECTED)
                .created(now.minusMinutes(10))
                .build());
        });

        var result = dao.findNextDownloadableItem();
        assertThat(result).isPresent();
        assertThat(result.get().getId()).isEqualTo(id1);
    }
}
