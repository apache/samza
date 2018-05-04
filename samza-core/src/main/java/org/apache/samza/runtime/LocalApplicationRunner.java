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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.DistributedLockWithState;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
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
  private final Set<StreamProcessor> processors = ConcurrentHashMap.newKeySet();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicInteger numProcessorsToStart = new AtomicInteger();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  private ApplicationStatus appStatus = ApplicationStatus.New;

  final class LocalStreamProcessorLifeCycleListener implements StreamProcessorLifecycleListener {
    StreamProcessor processor;

    void setProcessor(StreamProcessor processor) {
      this.processor = processor;
    }

    @Override
    public void onStart() {
      if (numProcessorsToStart.decrementAndGet() == 0) {
        appStatus = ApplicationStatus.Running;
      }
    }

    @Override
    public void onShutdown() {
      processors.remove(processor);
      processor = null;

      if (processors.isEmpty()) {
        shutdownAndNotify();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      processors.remove(processor);
      processor = null;

      if (failure.compareAndSet(null, t)) {
        // shutdown the other processors
        processors.forEach(StreamProcessor::stop);
      }

      if (processors.isEmpty()) {
        shutdownAndNotify();
      }
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

  public LocalApplicationRunner(Config config) {
    super(config);
    uid = UUID.randomUUID().toString();
  }

  @Override
  public void runTask() {
    JobConfig jobConfig = new JobConfig(this.config);

    // validation
    String taskName = new TaskConfig(config).getTaskClass().getOrElse(null);
    if (taskName == null) {
      throw new SamzaException("Neither APP nor task.class are defined defined");
    }
    LOG.info("LocalApplicationRunner will run " + taskName);
    LocalStreamProcessorLifeCycleListener listener = new LocalStreamProcessorLifeCycleListener();

    StreamProcessor processor = createStreamProcessor(jobConfig, null, listener);

    numProcessorsToStart.set(1);
    listener.setProcessor(processor);
    processor.start();
  }

  @Override
  public void run(StreamApplication app) {
    try {
      super.run(app);
      // 1. initialize and plan
      ExecutionPlan plan = getExecutionPlan(app);

      String executionPlanJson = plan.getPlanAsJson();
      writePlanJsonFile(executionPlanJson);
      LOG.info("Execution Plan: \n" + executionPlanJson);

      // 2. create the necessary streams
      // TODO: System generated intermediate streams should have robust naming scheme. See SAMZA-1391
      String planId = String.valueOf(executionPlanJson.hashCode());
      createStreams(planId, plan.getIntermediateStreams());

      // 3. create the StreamProcessors
      if (plan.getJobConfigs().isEmpty()) {
        throw new SamzaException("No jobs to run.");
      }
      plan.getJobConfigs().forEach(jobConfig -> {
          LOG.debug("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
          LocalStreamProcessorLifeCycleListener listener = new LocalStreamProcessorLifeCycleListener();
          StreamProcessor processor = createStreamProcessor(jobConfig, app, listener);
          listener.setProcessor(processor);
          processors.add(processor);
        });
      numProcessorsToStart.set(processors.size());

      // 4. start the StreamProcessors
      processors.forEach(StreamProcessor::start);
    } catch (Throwable throwable) {
      appStatus = ApplicationStatus.unsuccessfulFinish(throwable);
      shutdownLatch.countDown();
      throw new SamzaException(String.format("Failed to start application: %s.", app), throwable);
    }
  }

  @Override
  public void kill(StreamApplication streamApp) {
    processors.forEach(StreamProcessor::stop);
    super.kill(streamApp);
  }

  @Override
  public ApplicationStatus status(StreamApplication streamApp) {
    return appStatus;
  }

  /**
   * Waits until the application finishes.
   */
  @Override
  public void waitForFinish() {
    waitForFinish(Duration.ofMillis(0));
  }

  /**
   * Waits until the application finishes. It times out after the input duration has elapsed.
   * If timeout is zero or negative, then real time is not taken into consideration and the thread simply waits until notified.
   *
   * @param timeout time to wait for the application to finish
   * @return true - application finished before timeout
   *         false - otherwise
   */
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
          LOG.error("Waiting to shutdown local application runner timed out.");
        }
      }
    } catch (Exception e) {
      LOG.error("Wait for application finish failed due to", e);
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
  /* package private */ void createStreams(String planId, List<StreamSpec> intStreams) throws TimeoutException {
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
      getStreamManager().createStreams(intStreams);
      return;
    }

    DistributedLockWithState lockWithState = coordinationUtils.getLockWithState(planId);
    try {
      // check if the processor needs to go through leader election and stream creation
      if (lockWithState.lockIfNotSet(1000, TimeUnit.MILLISECONDS)) {
        LOG.info("lock acquired for streams creation by " + uid);
        getStreamManager().createStreams(intStreams);
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
   * Create {@link StreamProcessor} based on {@link StreamApplication} and the config
   * @param config config
   * @param app {@link StreamApplication}
   * @return {@link StreamProcessor]}
   */
  /* package private */
  StreamProcessor createStreamProcessor(
      Config config,
      StreamApplication app,
      StreamProcessorLifecycleListener listener) {
    Object taskFactory = TaskFactoryUtil.createTaskFactory(config, app, new LocalApplicationRunner(config));
    if (taskFactory instanceof StreamTaskFactory) {
      return new StreamProcessor(
          config, new HashMap<>(), (StreamTaskFactory) taskFactory, listener);
    } else if (taskFactory instanceof AsyncStreamTaskFactory) {
      return new StreamProcessor(
          config, new HashMap<>(), (AsyncStreamTaskFactory) taskFactory, listener);
    } else {
      throw new SamzaException(String.format("%s is not a valid task factory",
          taskFactory.getClass().getCanonicalName()));
    }
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
