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
import nl.knaw.dans.lobstore.core.Location;
import org.hibernate.SessionFactory;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.Optional;

public class LocationDao extends AbstractDAO<Location> {

    public LocationDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Location save(Location location) {
        return persist(location);
    }

    public Optional<Location> findByDatastationAndSha1Sum(String datastation, String sha1Sum) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<Location> cq = cb.createQuery(Location.class);
        Root<Location> root = cq.from(Location.class);
        cq.where(
            cb.equal(root.get("datastation"), datastation),
            cb.equal(root.get("sha1Sum"), sha1Sum)
        );
        return currentSession().createQuery(cq).setMaxResults(1).uniqueResultOptional();
    }
}
