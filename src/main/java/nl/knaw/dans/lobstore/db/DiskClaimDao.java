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
import nl.knaw.dans.lobstore.core.DiskClaim;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.UUID;

public class DiskClaimDao extends AbstractDAO<DiskClaim> {
    public DiskClaimDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<DiskClaim> findAll() {
        return list(currentSession().createQuery("from DiskClaim", DiskClaim.class));
    }

    public List<DiskClaim> findByFolder(String folder) {
        return list(currentSession().createQuery("from DiskClaim d where d.folder = :folder", DiskClaim.class)
                .setParameter("folder", folder));
    }

    public DiskClaim create(DiskClaim claim) {
        return persist(claim);
    }

    public void deleteByJobId(UUID jobId) {
        currentSession().createQuery("delete from DiskClaim d where d.jobId = :jobId")
                .setParameter("jobId", jobId)
                .executeUpdate();
    }

    public void deleteByJobIdAndFolder(UUID jobId, String folder) {
        currentSession().createQuery("delete from DiskClaim d where d.jobId = :jobId and d.folder = :folder")
                .setParameter("jobId", jobId)
                .setParameter("folder", folder)
                .executeUpdate();
    }
}
