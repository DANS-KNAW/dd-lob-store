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

import io.dropwizard.hibernate.AbstractDAO;
import nl.knaw.dans.lobstore.core.Job;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import nl.knaw.dans.lobstore.api.JobStatusDto;

public class JobDao extends AbstractDAO<Job> {
    public JobDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Optional<Job> findById(UUID id) {
        return Optional.ofNullable(get(id));
    }

    public Job create(Job job) {
        return persist(job);
    }

    public List<Job> findByStatus(JobStatusDto status) {
        return list(namedTypedQuery("nl.knaw.dans.lobstore.core.Job.findByStatus")
                .setParameter("status", status));
    }

    public Optional<Job> findBySha1SumAndStatus(String sha1Sum, JobStatusDto status) {
        return Optional.ofNullable(uniqueResult(namedTypedQuery("nl.knaw.dans.lobstore.core.Job.findBySha1SumAndStatus")
                .setParameter("sha1Sum", sha1Sum)
                .setParameter("status", status)));
    }
}
