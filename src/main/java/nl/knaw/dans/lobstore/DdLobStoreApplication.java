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

import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.lib.util.ExecutorServiceFactory;
import nl.knaw.dans.lib.util.pollingtaskexec.ExecutorServiceTaskScheduler;
import nl.knaw.dans.lib.util.pollingtaskexec.PollingTaskExecutor;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskFactory;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskScheduler;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskSource;
import nl.knaw.dans.lobstore.config.DdLobStoreConfig;
import nl.knaw.dans.lobstore.core.DiskQuotaManager;
import nl.knaw.dans.lobstore.core.DownloadTask;
import nl.knaw.dans.lobstore.core.DownloadTaskFactory;
import nl.knaw.dans.lobstore.core.DownloadTaskSource;
import nl.knaw.dans.lobstore.core.Job;
import nl.knaw.dans.lobstore.core.PackageTask;
import nl.knaw.dans.lobstore.core.PackageTaskFactory;
import nl.knaw.dans.lobstore.core.PackageTaskSource;
import nl.knaw.dans.lobstore.core.TransferTask;
import nl.knaw.dans.lobstore.core.TransferTaskFactory;
import nl.knaw.dans.lobstore.core.TransferTaskSource;
import nl.knaw.dans.lobstore.core.VerificationTask;
import nl.knaw.dans.lobstore.core.VerificationTaskFactory;
import nl.knaw.dans.lobstore.core.VerificationTaskSource;
import nl.knaw.dans.lobstore.db.DiskClaimDao;
import nl.knaw.dans.lobstore.db.JobDao;
import nl.knaw.dans.lobstore.resources.JobsResource;
import nl.knaw.dans.lobstore.resources.LocationResource;

import javax.ws.rs.client.Client;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

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

        DownloadTask downloadTask = new DownloadTask(jobDao, httpClient, config.getTransfer().getDownload().getBaseDir(), config.getTransfer().getDownload().getChunkSize().toBytes(),
            diskQuotaManager);
        PackageTask packageTask = new PackageTask(jobDao, config.getTransfer().getPackageConfig().getBaseDir(), config.getTransfer().getDownload().getMinimalBucketSize().toBytes(),
            config.getTransfer().getPackageConfig().getPackageCommand(), diskQuotaManager);
        TransferTask transferTask = new TransferTask(jobDao, config.getTransfer().getPackageConfig().getBaseDir(), config.getTransfer().getTransferJob().getTransferCommand(),
            config.getTransfer().getTransferJob().getDestination());
        VerificationTask verificationTask = new VerificationTask(jobDao, config.getTransfer().getDownload().getBaseDir(), config.getTransfer().getPackageConfig().getBaseDir(),
            config.getTransfer().getVerify().getSshCommand(), config.getTransfer().getVerify().getVerifyCommand(), config.getTransfer().getTransferJob().getDestination(), diskQuotaManager);

        environment.lifecycle().manage(
            createPollingTaskExecutor("Download", config.getTransfer().getDownload().getTaskQueue(), new DownloadTaskSource(jobDao), new DownloadTaskFactory(downloadTask), proxyFactory, environment));
        environment.lifecycle().manage(
            createPollingTaskExecutor("Package", config.getTransfer().getPackageConfig().getTaskQueue(), new PackageTaskSource(jobDao), new PackageTaskFactory(packageTask), proxyFactory,
                environment));
        environment.lifecycle().manage(
            createPollingTaskExecutor("Transfer", config.getTransfer().getTransferJob().getTaskQueue(), new TransferTaskSource(jobDao), new TransferTaskFactory(transferTask), proxyFactory,
                environment));
        environment.lifecycle().manage(
            createPollingTaskExecutor("Verify", config.getTransfer().getVerify().getTaskQueue(), new VerificationTaskSource(jobDao), new VerificationTaskFactory(verificationTask), proxyFactory,
                environment));
    }

    private Managed createPollingTaskExecutor(String name, ExecutorServiceFactory queueConfig, TaskSource<Job> source, TaskFactory<Job> factory, UnitOfWorkAwareProxyFactory proxyFactory,
        Environment environment) {
        ScheduledExecutorService scheduledExecutorService = environment.lifecycle().scheduledExecutorService(name + "-scheduler").build();
        TaskScheduler taskScheduler = new ExecutorServiceTaskScheduler(queueConfig.build(environment));

        PollingTaskExecutor<Job> executor = new PollingTaskExecutor<>(
            name,
            scheduledExecutorService,
            Duration.ofSeconds(10), // polling interval
            source,
            factory,
            taskScheduler);
        return proxyFactory.create(PollingTaskExecutor.class, new Class[] { PollingTaskExecutor.class }, new Object[] { executor });
    }
}
