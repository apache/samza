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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.SamzaException;
import org.apache.samza.application.AppDescriptorImpl;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.ApplicationDescriptors;
import org.apache.samza.application.StreamAppDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.DistributedLockWithState;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessor.StreamProcessorListenerSupplier;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link ApplicationRunner} that runs the applications in standalone environment
 */
public class LocalApplicationRunner extends AbstractApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(LocalApplicationRunner.class);
  private static final String APPLICATION_RUNNER_PATH_SUFFIX = "/ApplicationRunnerData";

  private final String uid;
  private final LocalJobPlanner planner;
  private final Set<StreamProcessor> processors = ConcurrentHashMap.newKeySet();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicInteger numProcessorsToStart = new AtomicInteger();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  private ApplicationStatus appStatus = ApplicationStatus.New;

  /**
   * Defines a specific implementation of {@link ProcessorLifecycleListener} for local {@link StreamProcessor}s.
   */
  private final class LocalStreamProcessorLifecycleListener implements ProcessorLifecycleListener {
    private final StreamProcessor processor;
    private final ProcessorLifecycleListener processorLifecycleListener;

    @Override
    public void beforeStart() {
      processorLifecycleListener.beforeStart();
    }

    @Override
    public void afterStart() {
      processorLifecycleListener.afterStart();
      if (numProcessorsToStart.decrementAndGet() == 0) {
        appStatus = ApplicationStatus.Running;
      }
    }

    @Override
    public void afterStop() {
      processors.remove(processor);

      processorLifecycleListener.afterStop();
      if (processors.isEmpty()) {
        // successful shutdown
        shutdownAndNotify();
      }
    }

    @Override
    public void afterFailure(Throwable t) {
      processors.remove(processor);

      processorLifecycleListener.afterFailure(t);
      // the processor stopped with failure
      if (failure.compareAndSet(null, t)) {
        // shutdown the other processors
        processors.forEach(StreamProcessor::stop);
      }

      if (processors.isEmpty()) {
        shutdownAndNotify();
      }
    }

    LocalStreamProcessorLifecycleListener(StreamProcessor processor) {
      this.processor = processor;
      this.processorLifecycleListener = appDesc.getProcessorLifecycleListenerFactory().
          createInstance(processor.getProcessorContext(), processor.getConfig());
    }

    private void shutdownAndNotify() {
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

      shutdownLatch.countDown();
    }

  }

  /**
   * Defines a {@link JobPlanner} with specific implementation of {@link JobPlanner#prepareStreamJobs(StreamAppDescriptorImpl)}
   * for standalone Samza processors.
   *
   * TODO: we need to consolidate all planning logic into {@link org.apache.samza.execution.ExecutionPlanner} after SAMZA-1811.
   */
  @VisibleForTesting
  static class LocalJobPlanner extends JobPlanner {
    private final String uid;

    LocalJobPlanner(AppDescriptorImpl descriptor, String uid) {
      super(descriptor);
      this.uid = uid;
    }

    @Override
    List<JobConfig> prepareStreamJobs(StreamAppDescriptorImpl streamAppDesc) throws Exception {
      // for high-level DAG, generating the plan and job configs
      StreamManager streamManager = null;
      try {
        streamManager = buildAndStartStreamManager();

        // 1. initialize and plan
        ExecutionPlan plan = getExecutionPlan(streamAppDesc.getOperatorSpecGraph(), streamManager);

        String executionPlanJson = plan.getPlanAsJson();
        writePlanJsonFile(executionPlanJson);
        LOG.info("Execution Plan: \n" + executionPlanJson);

        // 2. create the necessary streams
        // TODO: System generated intermediate streams should have robust naming scheme. See SAMZA-1391
        String planId = String.valueOf(executionPlanJson.hashCode());
        createStreams(planId, plan.getIntermediateStreams(), streamManager, config, uid);

        return plan.getJobConfigs();
      } finally {
        if (streamManager != null) {
          streamManager.stop();
        }
      }
    }
  }

  /**
   * Default constructor that is required by any implementation of {@link ApplicationRunner}
   *
   * @param userApp user application
   * @param config user configuration
   */
  public LocalApplicationRunner(ApplicationBase userApp, Config config) {
    super(ApplicationDescriptors.getAppDescriptor(userApp, config));
    this.uid = UUID.randomUUID().toString();
    this.planner = new LocalJobPlanner(appDesc, uid);
  }

  /**
   * Constructor only used in unit test to allow injection of {@link LocalJobPlanner}
   *
   */
  @VisibleForTesting
  LocalApplicationRunner(AppDescriptorImpl appDesc, LocalJobPlanner planner) {
    super(appDesc);
    this.uid = UUID.randomUUID().toString();
    this.planner = planner;
  }

  @Override
  public void run() {
    try {
      List<JobConfig> jobConfigs = planner.prepareJobs();
      // 3. create the StreamProcessors
      if (jobConfigs.isEmpty()) {
        throw new SamzaException("No jobs to run.");
      }
      jobConfigs.forEach(jobConfig -> {
          LOG.debug("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
          StreamProcessor processor = createStreamProcessor(jobConfig, appDesc, sp -> new LocalStreamProcessorLifecycleListener(sp));
          processors.add(processor);
        });
      numProcessorsToStart.set(processors.size());

      // 4. start the StreamProcessors
      processors.forEach(StreamProcessor::start);
    } catch (Throwable throwable) {
      appStatus = ApplicationStatus.unsuccessfulFinish(throwable);
      shutdownLatch.countDown();
      throw new SamzaException("Failed to start application.", throwable);
    }
  }

  @Override
  public void kill() {
    processors.forEach(StreamProcessor::stop);
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

    return finished;
  }

  /**
   * Create intermediate streams using {@link org.apache.samza.execution.StreamManager}.
   * If {@link CoordinationUtils} is provided, this function will first invoke leader election, and the leader
   * will create the streams. All the runner processes will wait on the latch that is released after the leader finishes
   * stream creation.
   * @param planId a unique identifier representing the plan used for coordination purpose
   * @param intStreams list of intermediate {@link StreamSpec}s
   * @throws TimeoutException exception for latch timeout
   */
  private static void createStreams(String planId, List<StreamSpec> intStreams, StreamManager streamManager,
      Config config, String uid) throws TimeoutException {
    if (intStreams.isEmpty()) {
      LOG.info("Set of intermediate streams is empty. Nothing to create.");
      return;
    }
    LOG.info("A single processor must create the intermediate streams. Processor {} will attempt to acquire the lock.", uid);
    // Move the scope of coordination utils within stream creation to address long idle connection problem.
    // Refer SAMZA-1385 for more details
    JobCoordinatorConfig jcConfig = new JobCoordinatorConfig(config);
    String coordinationId = new ApplicationConfig(config).getGlobalAppId() + APPLICATION_RUNNER_PATH_SUFFIX;
    CoordinationUtils coordinationUtils =
        jcConfig.getCoordinationUtilsFactory().getCoordinationUtils(coordinationId, uid, config);
    if (coordinationUtils == null) {
      LOG.warn("Processor {} failed to create utils. Each processor will attempt to create streams.", uid);
      // each application process will try creating the streams, which
      // requires stream creation to be idempotent
      streamManager.createStreams(intStreams);
      return;
    }

    DistributedLockWithState lockWithState = coordinationUtils.getLockWithState(planId);
    try {
      // check if the processor needs to go through leader election and stream creation
      if (lockWithState.lockIfNotSet(1000, TimeUnit.MILLISECONDS)) {
        LOG.info("lock acquired for streams creation by " + uid);
        streamManager.createStreams(intStreams);
        lockWithState.unlockAndSet();
      } else {
        LOG.info("Processor {} did not obtain the lock for streams creation. They must've been created by another processor.", uid);
      }
    } catch (TimeoutException e) {
      String msg = String.format("Processor {} failed to get the lock for stream initialization", uid);
      throw new SamzaException(msg, e);
    } finally {
      coordinationUtils.close();
    }
  }

  /**
   * Create {@link StreamProcessor} based on config, {@link AppDescriptorImpl}, and {@link StreamProcessorListenerSupplier}
   * @param config config
   * @param appDesc {@link AppDescriptorImpl}
   * @param listenerSupplier {@link StreamProcessorListenerSupplier} to create {@link ProcessorLifecycleListener}
   * @return {@link StreamProcessor]}
   */
  /* package private */
  StreamProcessor createStreamProcessor(Config config, AppDescriptorImpl appDesc, StreamProcessorListenerSupplier listenerSupplier) {
    TaskFactory taskFactory = TaskFactoryUtil.getTaskFactory(appDesc);
    return new StreamProcessor(config, this.metricsReporters, taskFactory, listenerSupplier, null);
  }

  /* package private for testing */
  Set<StreamProcessor> getProcessors() {
    return processors;
  }

  @VisibleForTesting
  CountDownLatch getShutdownLatch() {
    return shutdownLatch;
  }
}
