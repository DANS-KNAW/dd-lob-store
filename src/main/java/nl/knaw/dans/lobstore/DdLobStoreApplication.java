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
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.client.JerseyClientBuilder;
import nl.knaw.dans.lobstore.config.DdLobStoreConfig;
import nl.knaw.dans.lobstore.core.DiskQuotaManager;
import nl.knaw.dans.lobstore.core.DownloadTask;
import nl.knaw.dans.lobstore.core.PackageTask;
import nl.knaw.dans.lobstore.core.TransferTask;
import nl.knaw.dans.lobstore.core.VerificationTask;
import nl.knaw.dans.lobstore.db.DiskClaimDao;
import nl.knaw.dans.lobstore.db.JobDao;
import nl.knaw.dans.lobstore.resources.JobsResource;
import nl.knaw.dans.lobstore.resources.LocationResource;

import javax.ws.rs.client.Client;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
        final JobDao jobDao = new JobDao(hibernateBundle.getSessionFactory());
        final DiskClaimDao diskClaimDao = new DiskClaimDao(hibernateBundle.getSessionFactory());
        
        final Client httpClient = new JerseyClientBuilder(environment)
                .using(config.getHttpClient())
                .build(getName());

        final DiskQuotaManager diskQuotaManager = new DiskQuotaManager(
                diskClaimDao,
                config.getTransfer().getDownload().getBaseDir(),
                config.getTransfer().getPackageConfig().getBaseDir(),
                config.getTransfer().getDownload().getQuota().toBytes(),
                config.getTransfer().getPackageConfig().getQuota().toBytes(),
                false // enforcement
        );

        environment.jersey().register(new JobsResource(jobDao));
        environment.jersey().register(new LocationResource(jobDao));

        // Background tasks
        UnitOfWorkAwareProxyFactory proxyFactory = new UnitOfWorkAwareProxyFactory(hibernateBundle);
        
        DownloadTask downloadTask = proxyFactory.create(DownloadTask.class, 
                new Class[]{JobDao.class, Client.class, java.nio.file.Path.class, long.class, DiskQuotaManager.class},
                new Object[]{jobDao, httpClient, config.getTransfer().getDownload().getBaseDir(), config.getTransfer().getDownload().getChunkSize().toBytes(), diskQuotaManager});
        
        PackageTask packageTask = proxyFactory.create(PackageTask.class,
                new Class[]{JobDao.class, java.nio.file.Path.class, long.class, String.class, DiskQuotaManager.class},
                new Object[]{jobDao, config.getTransfer().getPackageConfig().getBaseDir(), config.getTransfer().getDownload().getMinimalBucketSize().toBytes(), config.getTransfer().getPackageConfig().getPackageCommand(), diskQuotaManager});
        
        TransferTask transferTask = proxyFactory.create(TransferTask.class,
                new Class[]{JobDao.class, java.nio.file.Path.class, String.class, String.class},
                new Object[]{jobDao, config.getTransfer().getPackageConfig().getBaseDir(), config.getTransfer().getTransferJob().getTransferCommand(), config.getTransfer().getTransferJob().getDestination()});
        
        VerificationTask verificationTask = proxyFactory.create(VerificationTask.class,
                new Class[]{JobDao.class, java.nio.file.Path.class, java.nio.file.Path.class, String.class, String.class, String.class, DiskQuotaManager.class},
                new Object[]{jobDao, config.getTransfer().getDownload().getBaseDir(), config.getTransfer().getPackageConfig().getBaseDir(), config.getTransfer().getVerify().getSshCommand(), config.getTransfer().getVerify().getVerifyCommand(), config.getTransfer().getTransferJob().getDestination(), diskQuotaManager});

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        scheduler.scheduleAtFixedRate(downloadTask, 0, 10000L, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(packageTask, 0, 10000L, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(transferTask, 0, 10000L, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(verificationTask, 0, 10000L, TimeUnit.MILLISECONDS);
        
        environment.lifecycle().manage(new ManagedExecutorService(scheduler));
    }

    private static class ManagedExecutorService implements io.dropwizard.lifecycle.Managed {
        private final ScheduledExecutorService scheduler;
        ManagedExecutorService(ScheduledExecutorService scheduler) { this.scheduler = scheduler; }
        @Override public void start() {}
        @Override public void stop() { scheduler.shutdown(); }
    }
}
