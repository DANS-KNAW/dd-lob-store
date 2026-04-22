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
import nl.knaw.dans.lobstore.core.Claim;
import org.hibernate.SessionFactory;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.List;
import java.util.Optional;

public class ClaimDao extends AbstractDAO<Claim> {

    public ClaimDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Claim save(Claim claim) {
        return persist(claim);
    }

    public Optional<Claim> findById(String id) {
        return Optional.ofNullable(get(id));
    }

    public void delete(Claim claim) {
        currentSession().delete(claim);
    }

    public Long sumSizeByTarget(String target) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<Long> cq = cb.createQuery(Long.class);
        Root<Claim> root = cq.from(Claim.class);
        cq.select(cb.sum(root.get("size")));
        cq.where(cb.equal(root.get("target"), target));
        Long result = currentSession().createQuery(cq).getSingleResult();
        return result == null ? 0L : result;
    }
}
