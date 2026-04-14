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
package nl.knaw.dans.lobstore;

import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import nl.knaw.dans.lobstore.config.DdLobStoreConfig;
import nl.knaw.dans.lobstore.core.DiskClaim;
import nl.knaw.dans.lobstore.core.Job;

public class DdLobStoreHibernateBundle extends HibernateBundle<DdLobStoreConfig> {
    public DdLobStoreHibernateBundle() {
        super(Job.class, DiskClaim.class);
    }

    @Override
    public PooledDataSourceFactory getDataSourceFactory(DdLobStoreConfig configuration) {
        return configuration.getDatabase();
    }
}
