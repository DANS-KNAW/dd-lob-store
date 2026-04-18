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

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.hibernate.HibernateBundle;
import nl.knaw.dans.lobstore.config.DdLobStoreConfig;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import nl.knaw.dans.lobstore.resources.DefaultResource;
import nl.knaw.dans.lobstore.resources.LocationResource;
import nl.knaw.dans.lobstore.resources.TransfersResource;

public class DdLobStoreApplication extends Application<DdLobStoreConfig> {

    private final HibernateBundle<DdLobStoreConfig> hibernateBundle = new DdLobStoreHibernateBundle();

    public static void main(final String[] args) throws Exception {
        new DdLobStoreApplication().run(args);
    }

    @Override
    public String getName() {
        return "DD LOB-store";
    }

    @Override
    public void initialize(final Bootstrap<DdLobStoreConfig> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
    }

    @Override
    public void run(final DdLobStoreConfig config, final Environment environment) {
        final TransferRequestDao transferRequestDao = new TransferRequestDao(hibernateBundle.getSessionFactory());
        environment.jersey().register(new TransfersResource(transferRequestDao));
        environment.jersey().register(new LocationResource());
        environment.jersey().register(new DefaultResource());
    }
}
