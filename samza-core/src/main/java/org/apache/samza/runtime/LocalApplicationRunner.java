/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.runtime;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.coordinator.CoordinationConstants;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.RunIdGenerator;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamMetadataStoreFactory;
import org.apache.samza.execution.LocalJobPlanner;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.apache.samza.zk.ZkMetadataStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link ApplicationRunner} that runs the applications in standalone environment
 */
public class LocalApplicationRunner implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(LocalApplicationRunner.class);
  private static final String PROCESSOR_ID = UUID.randomUUID().toString();
  private static final String RUN_ID_METADATA_STORE = "RunIdCoordinationStore";
  private static final String METADATA_STORE_FACTORY_CONFIG = "metadata.store.factory";
  private static final String DEFAULT_METADATA_STORE_FACTORY = ZkMetadataStoreFactory.class.getName();

  private final ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc;
  private final Set<Pair<StreamProcessor, MetadataStore>> processors = ConcurrentHashMap.newKeySet();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicInteger numProcessorsToStart = new AtomicInteger();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final boolean isAppModeBatch;
  private final Optional<CoordinationUtils> coordinationUtils;
  private final Optional<MetadataStoreFactory> metadataStoreFactory;

  private Optional<String> runId = Optional.empty();
  private Optional<RunIdGenerator> runIdGenerator = Optional.empty();
  private ApplicationStatus appStatus = ApplicationStatus.New;

  /**
   * Constructors a {@link LocalApplicationRunner} to run the {@code app} with the {@code config}.
   *
   * @param app application to run
   * @param config configuration for the application
   */
  public LocalApplicationRunner(SamzaApplication app, Config config) {
    this(app, config, getDefaultCoordinatorStreamStoreFactory(new JobConfig(config)));
  }

  /**
   * Constructors a {@link LocalApplicationRunner} to run the {@code app} with the {@code config}.
   *
   * @param app application to run
   * @param config configuration for the application
   * @param metadataStoreFactory the instance of {@link MetadataStoreFactory} to read and write to coordinator stream.
   */
  public LocalApplicationRunner(SamzaApplication app, Config config, MetadataStoreFactory metadataStoreFactory) {
    this(ApplicationDescriptorUtil.getAppDescriptor(app, config), getCoordinationUtils(config), metadataStoreFactory);
  }

  /**
   * Constructor only used in unit test to allow injection of {@link LocalJobPlanner}
   */
  @VisibleForTesting
  LocalApplicationRunner(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc, Optional<CoordinationUtils> coordinationUtils) {
    this(appDesc, coordinationUtils, getDefaultCoordinatorStreamStoreFactory(new JobConfig(appDesc.getConfig())));
  }

  private LocalApplicationRunner(
      ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      Optional<CoordinationUtils> coordinationUtils,
      MetadataStoreFactory metadataStoreFactory) {
    this.appDesc = appDesc;
    this.isAppModeBatch = isAppModeBatch(appDesc.getConfig());
    this.coordinationUtils = coordinationUtils;
    this.metadataStoreFactory = Optional.ofNullable(metadataStoreFactory);
  }

  @VisibleForTesting
  static MetadataStoreFactory getDefaultCoordinatorStreamStoreFactory(JobConfig jobConfig) {
    String coordinatorSystemName = jobConfig.getCoordinatorSystemNameOrNull();
    JobCoordinatorConfig jobCoordinatorConfig = new JobCoordinatorConfig(jobConfig);
    String jobCoordinatorFactoryClassName = jobCoordinatorConfig.getJobCoordinatorFactoryClassName();

    // TODO: Remove restriction to only ZkJobCoordinator after next phase of metadata store abstraction.
    if (StringUtils.isNotBlank(coordinatorSystemName) && ZkJobCoordinatorFactory.class.getName().equals(jobCoordinatorFactoryClassName)) {
      return new CoordinatorStreamMetadataStoreFactory();
    }

    LOG.warn("{} or {} not configured, or {} is not {}. No default coordinator stream metadata store will be created.",
        JobConfig.JOB_COORDINATOR_SYSTEM, JobConfig.JOB_DEFAULT_SYSTEM,
        JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, ZkJobCoordinatorFactory.class.getName());
    return null;
  }

  private static Optional<CoordinationUtils> getCoordinationUtils(Config config) {
    if (!isAppModeBatch(config)) {
      return Optional.empty();
    }
    JobCoordinatorConfig jcConfig = new JobCoordinatorConfig(config);
    CoordinationUtils coordinationUtils = jcConfig.getCoordinationUtilsFactory()
        .getCoordinationUtils(CoordinationConstants.APPLICATION_RUNNER_PATH_SUFFIX, PROCESSOR_ID, config);
    return Optional.ofNullable(coordinationUtils);
  }

  private static boolean isAppModeBatch(Config config) {
    return new ApplicationConfig(config).getAppMode() == ApplicationConfig.ApplicationMode.BATCH;
  }

  /**
   * @return LocalJobPlanner created
   */
  @VisibleForTesting
  LocalJobPlanner getPlanner() {
    boolean isAppModeBatch = new ApplicationConfig(appDesc.getConfig()).getAppMode() == ApplicationConfig.ApplicationMode.BATCH;
    if (!isAppModeBatch) {
      return new LocalJobPlanner(appDesc, PROCESSOR_ID);
    }
    CoordinationUtils coordinationUtils = this.coordinationUtils.orElse(null);
    String runId = this.runId.orElse(null);
    return new LocalJobPlanner(appDesc, coordinationUtils, PROCESSOR_ID, runId);
  }


  private void initializeRunId() {
    if (!isAppModeBatch) {
      LOG.info("Not BATCH mode and hence not generating run id");
      return;
    }

    if (!coordinationUtils.isPresent()) {
      LOG.warn("Coordination utils not present. Aborting run id generation. Will continue execution without a run id.");
      return;
    }

    try {
      MetadataStore metadataStore = getMetadataStoreForRunID();
      runIdGenerator = Optional.of(new RunIdGenerator(coordinationUtils.get(), metadataStore));
      runId = runIdGenerator.flatMap(RunIdGenerator::getRunId);
    } catch (Exception e) {
      LOG.warn("Failed to generate run id. Will continue execution without a run id.", e);
    }
  }

  public Optional<String> getRunId() {
    return this.runId;
  }

  @Override
  public void run(ExternalContext externalContext) {
    initializeRunId();

    LocalJobPlanner planner = getPlanner();

    try {
      List<JobConfig> jobConfigs = planner.prepareJobs();

      // create the StreamProcessors
      if (jobConfigs.isEmpty()) {
        throw new SamzaException("No jobs to run.");
      }
      jobConfigs.forEach(jobConfig -> {
          LOG.debug("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
          MetadataStore coordinatorStreamStore = createCoordinatorStreamStore(jobConfig);
          if (coordinatorStreamStore != null) {
            coordinatorStreamStore.init();
          }
          StreamProcessor processor = createStreamProcessor(jobConfig, appDesc,
              sp -> new LocalStreamProcessorLifecycleListener(sp, jobConfig), Optional.ofNullable(externalContext), coordinatorStreamStore);
          processors.add(Pair.of(processor, coordinatorStreamStore));
        });
      numProcessorsToStart.set(processors.size());

      // start the StreamProcessors
      processors.forEach(sp -> sp.getLeft().start());
    } catch (Throwable throwable) {
      cleanup();
      appStatus = ApplicationStatus.unsuccessfulFinish(throwable);
      shutdownLatch.countDown();
      throw new SamzaException(String.format("Failed to start application: %s",
          new ApplicationConfig(appDesc.getConfig()).getGlobalAppId()), throwable);
    }
  }

  @Override
  public void kill() {
    processors.forEach(sp -> {
        sp.getLeft().stop();    // Stop StreamProcessor

        // Coordinator stream isn't required so a null check is necessary
        if (sp.getRight() != null) {
          sp.getRight().close();  // Close associated coordinator metadata store
        }
      });
    cleanup();
  }

  @Override
  public ApplicationStatus status() {
    return appStatus;
  }

  @Override
  public void waitForFinish() {
    this.waitForFinish(Duration.ofSeconds(0));
  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    long timeoutInMs = timeout.toMillis();
    boolean finished = true;

    try {
      if (timeoutInMs < 1) {
        shutdownLatch.await();
      } else {
        finished = shutdownLatch.await(timeoutInMs, TimeUnit.MILLISECONDS);

        if (!finished) {
          LOG.warn("Timed out waiting for application to finish.");
        }
      }
    } catch (Exception e) {
      LOG.error("Error waiting for application to finish", e);
      throw new SamzaException(e);
    }

    cleanup();
    return finished;
  }

  @VisibleForTesting
  protected Set<StreamProcessor> getProcessors() {
    return processors.stream().map(Pair::getLeft).collect(Collectors.toSet());
  }

  @VisibleForTesting
  CountDownLatch getShutdownLatch() {
    return shutdownLatch;
  }

  @VisibleForTesting
  MetadataStore createCoordinatorStreamStore(Config config) {
    if (metadataStoreFactory.isPresent()) {
      // TODO: Add missing metadata store abstraction for creating the underlying store to address SAMZA-2182
      if (metadataStoreFactory.get() instanceof CoordinatorStreamMetadataStoreFactory) {
        if (createUnderlyingCoordinatorStream(config)) {
          MetadataStore coordinatorStreamStore =
              metadataStoreFactory.get().getMetadataStore("NoOp", config, new MetricsRegistryMap());
          LOG.info("Created coordinator stream store of type: {}", coordinatorStreamStore.getClass().getSimpleName());
          return coordinatorStreamStore;
        }
      } else {
        MetadataStore otherMetadataStore =
            metadataStoreFactory.get().getMetadataStore("NoOp", config, new MetricsRegistryMap());
        LOG.info("Created alternative coordinator stream store of type: {}", otherMetadataStore.getClass().getSimpleName());
        return otherMetadataStore;
      }
    }

    LOG.warn("No coordinator stream store created.");
    return null;
  }

  @VisibleForTesting
  boolean createUnderlyingCoordinatorStream(Config config) {
    // TODO: This work around method is necessary due to SAMZA-2182 - Metadata store: disconnect between creation and usage of the underlying storage
    //  and will be addressed in the next phase of metadata store abstraction
    if (new JobConfig(config).getCoordinatorSystemNameOrNull() == null) {
      LOG.warn("{} or {} not configured. Coordinator stream not created.",
          JobConfig.JOB_COORDINATOR_SYSTEM, JobConfig.JOB_DEFAULT_SYSTEM);
      return false;
    }
    SystemStream coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(config);
    SystemAdmins systemAdmins = new SystemAdmins(config);
    systemAdmins.start();
    try {
      SystemAdmin coordinatorSystemAdmin = systemAdmins.getSystemAdmin(coordinatorSystemStream.getSystem());
      CoordinatorStreamUtil.createCoordinatorStream(coordinatorSystemStream, coordinatorSystemAdmin);
    } finally {
      systemAdmins.stop();
    }
    return true;
  }

  @VisibleForTesting
  StreamProcessor createStreamProcessor(Config config, ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      StreamProcessor.StreamProcessorLifecycleListenerFactory listenerFactory,
      Optional<ExternalContext> externalContextOptional, MetadataStore coordinatorStreamStore) {
    TaskFactory taskFactory = TaskFactoryUtil.getTaskFactory(appDesc);
    Map<String, MetricsReporter> reporters = new HashMap<>();
    String processorId = createProcessorId(new ApplicationConfig(config));
    appDesc.getMetricsReporterFactories().forEach((name, factory) ->
        reporters.put(name, factory.getMetricsReporter(name, processorId, config)));

    return new StreamProcessor(processorId, config, reporters, taskFactory, appDesc.getApplicationContainerContextFactory(),
          appDesc.getApplicationTaskContextFactory(), externalContextOptional, listenerFactory, null, coordinatorStreamStore);
  }

  /**
   * Generates a unique logical identifier for the stream processor using the provided {@param appConfig}.
   * 1. If the processorId is defined in the configuration, then returns the value defined in the configuration.
   * 2. Else if the {@linkplain ProcessorIdGenerator} class is defined the configuration, then uses the {@linkplain ProcessorIdGenerator}
   * to generate the unique processorId.
   * 3. Else throws the {@see ConfigException} back to the caller.
   * @param appConfig the configuration of the samza application.
   * @throws ConfigException if neither processor.id nor app.processor-id-generator.class is defined in the configuration.
   * @return the generated processor identifier.
   */
  @VisibleForTesting
  static String createProcessorId(ApplicationConfig appConfig) {
    if (StringUtils.isNotBlank(appConfig.getProcessorId())) {
      return appConfig.getProcessorId();
    } else if (StringUtils.isNotBlank(appConfig.getAppProcessorIdGeneratorClass())) {
      ProcessorIdGenerator idGenerator =
          ReflectionUtil.getObj(appConfig.getAppProcessorIdGeneratorClass(), ProcessorIdGenerator.class);
      return idGenerator.generateProcessorId(appConfig);
    } else {
      throw new ConfigException(String.format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
              ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
    }
  }

  private void cleanup() {
    runIdGenerator.ifPresent(RunIdGenerator::close);
    coordinationUtils.ifPresent(CoordinationUtils::close);
  }

  /**
   * This is not to be confused with the metadata store created from the member {@link #metadataStoreFactory}.
   * The reason for the two Metadata store types (ZK and coordinator stream) is that the job model needs to be stored in
   * ZK because of the versioning requirements. Configs and startpoints are stored in the coordinator stream. This
   * disparity will be resolved with the next gen metadata store abstraction.
   */
  private MetadataStore getMetadataStoreForRunID() {
    String metadataStoreFactoryClass = appDesc.getConfig().getOrDefault(METADATA_STORE_FACTORY_CONFIG, DEFAULT_METADATA_STORE_FACTORY);
    MetadataStoreFactory metadataStoreFactory =
        ReflectionUtil.getObj(metadataStoreFactoryClass, MetadataStoreFactory.class);
    return metadataStoreFactory.getMetadataStore(RUN_ID_METADATA_STORE, appDesc.getConfig(), new MetricsRegistryMap());
  }

  /**
   * Defines a specific implementation of {@link ProcessorLifecycleListener} for local {@link StreamProcessor}s.
   */
  private final class LocalStreamProcessorLifecycleListener implements ProcessorLifecycleListener {
    private final StreamProcessor processor;
    private final ProcessorLifecycleListener userDefinedProcessorLifecycleListener;

    LocalStreamProcessorLifecycleListener(StreamProcessor processor, Config jobConfig) {
      this.userDefinedProcessorLifecycleListener = appDesc.getProcessorLifecycleListenerFactory()
          .createInstance(new ProcessorContext() { }, jobConfig);
      this.processor = processor;
    }

    @Override
    public void beforeStart() {
      userDefinedProcessorLifecycleListener.beforeStart();
    }

    @Override
    public void afterStart() {
      if (numProcessorsToStart.decrementAndGet() == 0) {
        appStatus = ApplicationStatus.Running;
      }
      userDefinedProcessorLifecycleListener.afterStart();
    }

    @Override
    public void afterStop() {
      processors.removeIf(pair -> pair.getLeft().equals(processor));

      // successful shutdown
      handleProcessorShutdown(null);
    }

    @Override
    public void afterFailure(Throwable t) {
      processors.removeIf(pair -> pair.getLeft().equals(processor));

      // the processor stopped with failure, this is logging the first processor's failure as the cause of
      // the whole application failure
      if (failure.compareAndSet(null, t)) {
        // shutdown the other processors
        processors.forEach(sp -> {
            sp.getLeft().stop();    // Stop StreamProcessor
            sp.getRight().close();  // Close associated coordinator metadata store
          });
      }

      // handle the current processor's shutdown failure.
      handleProcessorShutdown(t);
    }

    private void handleProcessorShutdown(Throwable error) {
      if (processors.isEmpty()) {
        // all processors are shutdown, setting the application final status
        setApplicationFinalStatus();
      }
      if (error != null) {
        // current processor shutdown with a failure
        userDefinedProcessorLifecycleListener.afterFailure(error);
      } else {
        // current processor shutdown successfully
        userDefinedProcessorLifecycleListener.afterStop();
      }
      if (processors.isEmpty()) {
        cleanup();
        // no processor is still running. Notify callers waiting on waitForFinish()
        shutdownLatch.countDown();
      }
    }

    private void setApplicationFinalStatus() {
      if (failure.get() != null) {
        appStatus = ApplicationStatus.unsuccessfulFinish(failure.get());
      } else {
        if (appStatus == ApplicationStatus.Running) {
          appStatus = ApplicationStatus.SuccessfulFinish;
        } else if (appStatus == ApplicationStatus.New) {
          // the processor is shutdown before started
          appStatus = ApplicationStatus.UnsuccessfulFinish;
        }
      }
    }
  }
}
