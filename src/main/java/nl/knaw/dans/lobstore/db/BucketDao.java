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
import nl.knaw.dans.lobstore.core.Bucket;
import nl.knaw.dans.lobstore.core.BucketStatus;
import org.hibernate.SessionFactory;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class BucketDao extends AbstractDAO<Bucket> {

    public BucketDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<Bucket> findByStatus(BucketStatus status) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<Bucket> cq = cb.createQuery(Bucket.class);
        Root<Bucket> root = cq.from(Bucket.class);
        cq.where(cb.equal(root.get("status"), status));
        cq.orderBy(cb.asc(root.get("id")));
        return currentSession().createQuery(cq).getResultList();
    }

    public Bucket save(Bucket bucket) {
        return persist(bucket);
    }

    public Optional<Bucket> findById(UUID id) {
        return Optional.ofNullable(get(id));
    }
}
