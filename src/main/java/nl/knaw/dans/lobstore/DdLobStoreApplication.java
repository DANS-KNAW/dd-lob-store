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
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.util.pollingtaskexec.ExecutorServiceTaskScheduler;
import nl.knaw.dans.lib.util.pollingtaskexec.PollingTaskExecutor;
import nl.knaw.dans.lobstore.config.DdLobStoreConfig;
import nl.knaw.dans.lobstore.core.ActiveTaskRegistry;
import nl.knaw.dans.lobstore.core.DownloadTaskFactory;
import nl.knaw.dans.lobstore.core.DownloadTaskSource;
import nl.knaw.dans.lobstore.core.InspectTaskFactory;
import nl.knaw.dans.lobstore.core.InspectTaskSource;
import nl.knaw.dans.lobstore.core.PackagingTaskFactory;
import nl.knaw.dans.lobstore.core.PackagingTaskSource;
import nl.knaw.dans.lobstore.core.QuotaManager;
import nl.knaw.dans.lobstore.core.TransferRequest;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.ClaimDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;
import nl.knaw.dans.lobstore.resources.DefaultResource;
import nl.knaw.dans.lobstore.resources.LocationResource;
import nl.knaw.dans.lobstore.resources.TransfersResource;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

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
        final ClaimDao claimDao = new ClaimDao(hibernateBundle.getSessionFactory());
        final BucketDao bucketDao = new BucketDao(hibernateBundle.getSessionFactory());

        var uowProxyFactory = new UnitOfWorkAwareProxyFactory(hibernateBundle);
        final QuotaManager quotaManager = new QuotaManager(claimDao,
            config.getDiskSpace());

        final ActiveTaskRegistry downloadActiveTaskRegistry = new ActiveTaskRegistry();
        final ActiveTaskRegistry inspectActiveTaskRegistry = new ActiveTaskRegistry();
        final ActiveTaskRegistry packagingActiveTaskRegistry = new ActiveTaskRegistry();

        environment.jersey().register(new TransfersResource(transferRequestDao));
        environment.jersey().register(new LocationResource());
        environment.jersey().register(new DefaultResource());

        Map<String, DataverseClient> dataverseClients = config.getDatastations().entrySet().stream()
            .map(e -> Map.entry(e.getKey(),
                e.getValue().getDataverse().build(environment, e.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final ExecutorService chunkDownloadExecutor = environment.lifecycle()
            .executorService("chunk-download-executor")
            .minThreads(config.getTransfer().getDownload().getMaxTotalChunkThreads())
            .maxThreads(config.getTransfer().getDownload().getMaxTotalChunkThreads())
            .build();

        final PollingTaskExecutor<TransferRequest> inspectTaskExecutor = new PollingTaskExecutor<>(
            "InspectTaskExecutor",
            environment.lifecycle().scheduledExecutorService("inspect-task-executor", true).build(),
            config.getTransfer().getInspect().getPollingInterval().toJavaDuration(),
            new InspectTaskSource(transferRequestDao, inspectActiveTaskRegistry),
            new InspectTaskFactory(transferRequestDao, dataverseClients, inspectActiveTaskRegistry, uowProxyFactory),
            new ExecutorServiceTaskScheduler(config.getTransfer().getInspect().getTaskQueue().build(environment)));

        final PollingTaskExecutor<TransferRequest> downloadTaskExecutor = new PollingTaskExecutor<>(
            "DownloadTaskExecutor",
            environment.lifecycle().scheduledExecutorService("download-task-executor", true).build(),
            config.getTransfer().getDownload().getPollingInterval().toJavaDuration(),
            new DownloadTaskSource(transferRequestDao, quotaManager, downloadActiveTaskRegistry, config.getTransfer().getDownload().getMargin().toBytes()),
            new DownloadTaskFactory(transferRequestDao, dataverseClients, config.getTransfer().getDownload(), quotaManager, downloadActiveTaskRegistry, uowProxyFactory, chunkDownloadExecutor),
            new ExecutorServiceTaskScheduler(config.getTransfer().getDownload().getTaskQueue().build(environment)));

        final PollingTaskExecutor<TransferRequest> packagingTaskExecutor = new PollingTaskExecutor<>(
            "PackagingTaskExecutor",
            environment.lifecycle().scheduledExecutorService("packaging-task-executor", true).build(),
            config.getTransfer().getPackageConfig().getPollingInterval().toJavaDuration(),
            new PackagingTaskSource(transferRequestDao, bucketDao, quotaManager, packagingActiveTaskRegistry,
                config.getTransfer().getPackageConfig().getMinimalBucketSize().toBytes(),
                config.getTransfer().getPackageConfig().getMargin().toBytes()),
            new PackagingTaskFactory(bucketDao, transferRequestDao, config.getTransfer().getDownload(),
                config.getTransfer().getPackageConfig(), quotaManager, packagingActiveTaskRegistry, uowProxyFactory),
            new ExecutorServiceTaskScheduler(config.getTransfer().getPackageConfig().getTaskQueue().build(environment)));

        environment.lifecycle().manage(createUnitOfWorkAwareProxy(uowProxyFactory, inspectTaskExecutor));
        environment.lifecycle().manage(createUnitOfWorkAwareProxy(uowProxyFactory, downloadTaskExecutor));
        environment.lifecycle().manage(createUnitOfWorkAwareProxy(uowProxyFactory, packagingTaskExecutor));
    }

    private <R> PollingTaskExecutor<R> createUnitOfWorkAwareProxy(UnitOfWorkAwareProxyFactory uowFactory, PollingTaskExecutor<R> executor) {
        return uowFactory
            .create(PollingTaskExecutor.class, new Class<?>[] { PollingTaskExecutor.class }, new Object[] { executor });
    }

}
