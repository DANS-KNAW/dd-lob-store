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
import nl.knaw.dans.lobstore.core.TransferRequest;
import nl.knaw.dans.lobstore.core.TransferRequestStatus;
import org.hibernate.SessionFactory;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class TransferRequestDao extends AbstractDAO<TransferRequest> {

    public TransferRequestDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public TransferRequest save(TransferRequest transferRequest) {
        return persist(transferRequest);
    }

    public Optional<TransferRequest> findById(UUID id) {
        return Optional.ofNullable(get(id));
    }

    public List<TransferRequest> findBySha1Sum(String sha1Sum) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<TransferRequest> cq = cb.createQuery(TransferRequest.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);
        cq.where(cb.equal(root.get("sha1Sum"), sha1Sum));
        return currentSession().createQuery(cq).getResultList();
    }

    public List<TransferRequest> findByDataverseFileId(long id) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<TransferRequest> cq = cb.createQuery(TransferRequest.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);
        cq.where(cb.equal(root.get("dataverseFileId"), id));
        return currentSession().createQuery(cq).getResultList();
    }

    public List<TransferRequest> findInspectableItems() {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<TransferRequest> cq = cb.createQuery(TransferRequest.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);
        cq.where(cb.equal(root.get("status"), TransferRequestStatus.PENDING));
        cq.orderBy(cb.asc(root.get("created")));
        return currentSession().createQuery(cq).getResultList();
    }

    public Optional<TransferRequest> findNextInspectableItem() {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<TransferRequest> cq = cb.createQuery(TransferRequest.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);
        cq.where(root.get("status").in(TransferRequestStatus.PENDING));
        cq.orderBy(cb.asc(root.get("created")));
        return currentSession().createQuery(cq).setMaxResults(1).uniqueResultOptional();
    }

    public Optional<TransferRequest> findNextDownloadableItem() {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<TransferRequest> cq = cb.createQuery(TransferRequest.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);
        cq.where(root.get("status").in(TransferRequestStatus.INSPECTED, TransferRequestStatus.DOWNLOADING));
        cq.orderBy(cb.asc(root.get("created")));
        return currentSession().createQuery(cq).setMaxResults(1).uniqueResultOptional();
    }

    public List<TransferRequest> findPackagableItems() {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<TransferRequest> cq = cb.createQuery(TransferRequest.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);
        cq.where(cb.equal(root.get("status"), TransferRequestStatus.DOWNLOADED),
            cb.isNull(root.get("bucket")));
        cq.orderBy(cb.asc(root.get("created")));
        return currentSession().createQuery(cq).getResultList();
    }

    public List<String> findDatastationsReadyForPackaging(long minimalBucketSize) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<String> cq = cb.createQuery(String.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);

        // Filter: DOWNLOADED status and no bucket assigned
        cq.where(
            cb.equal(root.get("status"), TransferRequestStatus.DOWNLOADED),
            cb.isNull(root.get("bucket"))
        );

        // Group by datastation
        cq.select(root.get("datastation"));
        cq.groupBy(root.get("datastation"));

        // Having sum(fileSize) >= minimalBucketSize
        cq.having(cb.ge(cb.sum(root.get("fileSize")), minimalBucketSize));

        // Order by the oldest created date in the group (MIN(created))
        cq.orderBy(cb.asc(cb.least(root.get("created").as(OffsetDateTime.class))));

        return currentSession().createQuery(cq).getResultList();
    }

    public List<TransferRequest> findPackagableItemsByDatastation(String datastation) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<TransferRequest> cq = cb.createQuery(TransferRequest.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);

        cq.where(
            cb.equal(root.get("status"), TransferRequestStatus.DOWNLOADED),
            cb.isNull(root.get("bucket")),
            cb.equal(root.get("datastation"), datastation)
        );

        cq.orderBy(cb.asc(root.get("created")));

        return currentSession().createQuery(cq).getResultList();
    }

    private Optional<TransferRequest> findNextItemWithStatus(TransferRequestStatus status) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<TransferRequest> cq = cb.createQuery(TransferRequest.class);
        Root<TransferRequest> root = cq.from(TransferRequest.class);
        cq.where(cb.equal(root.get("status"), status));
        cq.orderBy(cb.asc(root.get("created")));
        return currentSession().createQuery(cq).setMaxResults(1).uniqueResultOptional();
    }

}
