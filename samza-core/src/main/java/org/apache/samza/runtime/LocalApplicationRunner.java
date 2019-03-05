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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.coordinator.CoordinationSessionListener;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.DistributedDataAccess;
import org.apache.samza.coordinator.DistributedReadWriteLock;
import org.apache.samza.execution.LocalJobPlanner;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link ApplicationRunner} that runs the applications in standalone environment
 */
public class LocalApplicationRunner implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(LocalApplicationRunner.class);
  private static final String RUNID_PATH = "runId";
  private static final String APPLICATION_RUNNER_PATH_SUFFIX = "/ApplicationRunnerData";
  private static final int LOCK_TIMEOUT = 10;
  private static final TimeUnit LOCK_TIMEOUT_UNIT = TimeUnit.MINUTES;

  private final ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc;
  private final LocalJobPlanner planner;
  private final Set<StreamProcessor> processors = ConcurrentHashMap.newKeySet();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicInteger numProcessorsToStart = new AtomicInteger();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final String uid = UUID.randomUUID().toString();
  private CoordinationUtils coordinationUtils = null;
  private DistributedReadWriteLock runIdLock = null;
  private String runId = null;

  private ApplicationStatus appStatus = ApplicationStatus.New;

  /**
   * Constructors a {@link LocalApplicationRunner} to run the {@code app} with the {@code config}.
   *
   * @param app application to run
   * @param config configuration for the application
   */
  public LocalApplicationRunner(SamzaApplication app, Config config) {
    this.appDesc = ApplicationDescriptorUtil.getAppDescriptor(app, config);
    this.coordinationUtils = getCoordinationUtils(config);
    getRunId();
    this.planner = new LocalJobPlanner(appDesc, coordinationUtils, uid, runId);
  }

  /**
   * Constructor only used in unit test to allow injection of {@link LocalJobPlanner}
   */
  @VisibleForTesting
  LocalApplicationRunner(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc, LocalJobPlanner planner, CoordinationUtils coordinationUtils) {
    this.appDesc = appDesc;
    this.planner = planner;
    this.coordinationUtils = coordinationUtils;
  }

  private CoordinationUtils getCoordinationUtils(Config config) {
    JobCoordinatorConfig jcConfig = new JobCoordinatorConfig(config);
    String coordinationId = new ApplicationConfig(config).getGlobalAppId() + APPLICATION_RUNNER_PATH_SUFFIX;
    return jcConfig.getCoordinationUtilsFactory().getCoordinationUtils(coordinationId, uid, config);
  }

  private void getRunId(){
    ApplicationConfig.ApplicationMode appMode = new ApplicationConfig(appDesc.getConfig()).getAppMode();
    if(coordinationUtils == null || appMode == ApplicationConfig.ApplicationMode.STREAM) {
      return;
    }

    runIdLock = coordinationUtils.getReadWriteLock();
    if(runIdLock == null) {
      LOG.warn("Processor {} failed to create the lock for run.id generation", uid);
      return;
    }

    DistributedDataAccess runIdAccess = coordinationUtils.getDataAccess();
    if(runIdAccess == null) {
      LOG.warn("Processor {} failed to create data access utils for run.id generation", uid);
      return;
    }

    coordinationUtils.setCoordinationSessionListener(new LocalCoordinationSessionListener());

    try {
      // acquire lock to write or read run.id
      DistributedReadWriteLock.AccessType lockAccess = runIdLock.lock(LOCK_TIMEOUT, LOCK_TIMEOUT_UNIT);
      if(lockAccess == DistributedReadWriteLock.AccessType.WRITE) {
        LOG.info("write lock acquired for run.id generation by Processor " + uid);
        runId = String.valueOf(System.currentTimeMillis()) + "-" + UUID.randomUUID().toString().substring(0, 8);
        LOG.info("The run id for this run is {}", runId);
        runIdAccess.writeData(RUNID_PATH,runId);
        runIdLock.unlock();
        return;
      } else if(lockAccess == DistributedReadWriteLock.AccessType.READ) {
        LOG.info("read lock acquired for run.id by Processor " + uid);
        runId = (String) runIdAccess.readData(RUNID_PATH);
        runIdLock.unlock();
        return;
      } else {
        String msg = String.format("Processor {} failed to get the lock for run.id", uid);
        throw new SamzaException(msg);
      }
    } catch (TimeoutException e) {
      String msg = String.format("Processor {} failed to get the lock for run.id generation", uid);
      throw new SamzaException(msg, e);
    }
  }

  @Override
  public void run(ExternalContext externalContext) {
    try {
      List<JobConfig> jobConfigs = planner.prepareJobs();

      ApplicationConfig.ApplicationMode appMode = new ApplicationConfig(appDesc.getConfig()).getAppMode();
      if(coordinationUtils != null && appMode == ApplicationConfig.ApplicationMode.STREAM) {
        coordinationUtils.close();
        coordinationUtils = null;
      }
      // create the StreamProcessors
      if (jobConfigs.isEmpty()) {
        throw new SamzaException("No jobs to run.");
      }
      jobConfigs.forEach(jobConfig -> {
          LOG.debug("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
          StreamProcessor processor = createStreamProcessor(jobConfig, appDesc,
              sp -> new LocalStreamProcessorLifecycleListener(sp, jobConfig), Optional.ofNullable(externalContext));
          processors.add(processor);
        });
      numProcessorsToStart.set(processors.size());

      // start the StreamProcessors
      processors.forEach(StreamProcessor::start);
    } catch (Throwable throwable) {
      appStatus = ApplicationStatus.unsuccessfulFinish(throwable);
      shutdownLatch.countDown();
      cleanup();
      throw new SamzaException(String.format("Failed to start application: %s",
          new ApplicationConfig(appDesc.getConfig()).getGlobalAppId()), throwable);
    }
  }

  @Override
  public void kill() {
    processors.forEach(StreamProcessor::stop);
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

    cleanup();

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

    return finished;
  }

  @VisibleForTesting
  protected Set<StreamProcessor> getProcessors() {
    return Collections.unmodifiableSet(processors);
  }

  @VisibleForTesting
  CountDownLatch getShutdownLatch() {
    return shutdownLatch;
  }

  @VisibleForTesting
  StreamProcessor createStreamProcessor(Config config, ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      StreamProcessor.StreamProcessorLifecycleListenerFactory listenerFactory,
      Optional<ExternalContext> externalContextOptional) {
    TaskFactory taskFactory = TaskFactoryUtil.getTaskFactory(appDesc);
    Map<String, MetricsReporter> reporters = new HashMap<>();
    String processorId = createProcessorId(new ApplicationConfig(config));
    appDesc.getMetricsReporterFactories().forEach((name, factory) ->
        reporters.put(name, factory.getMetricsReporter(name, processorId, config)));
    return new StreamProcessor(processorId, config, reporters, taskFactory, appDesc.getApplicationContainerContextFactory(),
        appDesc.getApplicationTaskContextFactory(), externalContextOptional, listenerFactory, null);
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
      ProcessorIdGenerator idGenerator = Util.getObj(appConfig.getAppProcessorIdGeneratorClass(), ProcessorIdGenerator.class);
      return idGenerator.generateProcessorId(appConfig);
    } else {
      throw new ConfigException(String.format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
              ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
    }
  }

  private void cleanup() {
    if(runIdLock != null) {
      runIdLock.cleanState();
    }
    if(coordinationUtils != null) {
      coordinationUtils.close();
    }
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
      processors.remove(processor);

      // successful shutdown
      handleProcessorShutdown(null);
    }

    @Override
    public void afterFailure(Throwable t) {
      processors.remove(processor);

      // the processor stopped with failure, this is logging the first processor's failure as the cause of
      // the whole application failure
      if (failure.compareAndSet(null, t)) {
        // shutdown the other processors
        processors.forEach(StreamProcessor::stop);
      }

      // handle the current processor's shutdown failure.
      handleProcessorShutdown(t);
    }

    private void handleProcessorShutdown(Throwable error) {
      cleanup();
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


  /**
   * Defines a specific implementation of {@link CoordinationSessionListener} for local {@link CoordinationUtils}
   */
  private final class LocalCoordinationSessionListener implements CoordinationSessionListener {

    /**
     * If the coordination utils session has reconnected, check if global runid differs from local runid
     * if it differs then shut down processor and throw exception
     * else recreate ephemeral node corresponding to this processor inside the read write lock for runid
     */
    @Override
    public void handleReconnect() {
      LOG.info("Reconnected to coordination utils");
      if(coordinationUtils == null) {
        return;
      }
      DistributedDataAccess runIdAccess = coordinationUtils.getDataAccess();
      String globalRunId = (String) runIdAccess.readData(RUNID_PATH);
      if( runId != globalRunId){
        processors.forEach(StreamProcessor::stop);
        cleanup();
        appStatus = ApplicationStatus.UnsuccessfulFinish;
        String msg = String.format("run.id %s on processor %s differs from the global run.id %s", runId, uid, globalRunId);
        throw new SamzaException(msg);
      } else if(runIdLock != null) {
        String msg = String.format("Processor {} failed to get the lock for run.id", uid);
        try {
          // acquire lock to recreate active processor ephemeral node
          DistributedReadWriteLock.AccessType lockAccess = runIdLock.lock(LOCK_TIMEOUT, LOCK_TIMEOUT_UNIT);
          if(lockAccess == DistributedReadWriteLock.AccessType.WRITE || lockAccess == DistributedReadWriteLock.AccessType.READ) {
            LOG.info("Processor {} creates its active processor ephemeral node in read write lock again" + uid);
            runIdLock.unlock();
          } else {
            throw new SamzaException(msg);
          }
        } catch (TimeoutException e) {
          throw new SamzaException(msg, e);
        }
      }
    }
  }
}
