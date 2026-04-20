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
import nl.knaw.dans.lobstore.core.Location;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
class LocationDaoTest {

    private final DAOTestExtension db = DAOTestExtension.newBuilder()
        .addEntityClass(Location.class)
        .build();

    private LocationDao dao;

    @BeforeEach
    void setUp() {
        dao = new LocationDao(db.getSessionFactory());
    }

    @Test
    void findByDatastationAndSha1Sum_should_return_matching_location() {
        String datastation = "station1";
        String sha1Sum = "sha1";
        String bucketName = "bucket1";

        db.inTransaction(() -> {
            dao.save(Location.builder()
                .datastation(datastation)
                .sha1Sum(sha1Sum)
                .bucketName(bucketName)
                .build());

            dao.save(Location.builder()
                .datastation("other")
                .sha1Sum(sha1Sum)
                .bucketName("bucket2")
                .build());

            dao.save(Location.builder()
                .datastation(datastation)
                .sha1Sum("other")
                .bucketName("bucket3")
                .build());
        });

        Optional<Location> result = dao.findByDatastationAndSha1Sum(datastation, sha1Sum);
        assertThat(result).isPresent();
        assertThat(result.get().getBucketName()).isEqualTo(bucketName);
    }

    @Test
    void findByDatastationAndSha1Sum_should_return_empty_if_no_match() {
        Optional<Location> result = dao.findByDatastationAndSha1Sum("nonexistent", "nonexistent");
        assertThat(result).isEmpty();
    }
}
