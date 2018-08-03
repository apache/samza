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
import org.apache.samza.application.internal.StreamAppSpecImpl;
import org.apache.samza.application.internal.TaskAppSpecImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.DistributedLockWithState;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskFactory;
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

  public LocalApplicationRunner(Config config) {
    super(config);
    this.uid = UUID.randomUUID().toString();
  }

  private final class LocalStreamProcessorLifeCycleListener implements StreamProcessorLifecycleListener {
    private StreamProcessor processor;

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

  private class StreamAppExecutable implements AppRuntimeExecutable {
    private final StreamAppSpecImpl streamApp;

    private StreamAppExecutable(StreamAppSpecImpl streamApp) {
      this.streamApp = streamApp;
    }

    @Override
    public void run() {
      StreamManager streamManager = null;
      try {
        streamManager = buildAndStartStreamManager();

        // 1. initialize and plan
        ExecutionPlan plan = getExecutionPlan(((StreamGraphSpec)streamApp.getGraph()).getOperatorSpecGraph(), streamManager);

        String executionPlanJson = plan.getPlanAsJson();
        writePlanJsonFile(executionPlanJson);
        LOG.info("Execution Plan: \n" + executionPlanJson);

        // 2. create the necessary streams
        // TODO: System generated intermediate streams should have robust naming scheme. See SAMZA-1391
        String planId = String.valueOf(executionPlanJson.hashCode());
        createStreams(planId, plan.getIntermediateStreams(), streamManager);

        // 3. create the StreamProcessors
        if (plan.getJobConfigs().isEmpty()) {
          throw new SamzaException("No jobs to start.");
        }
        plan.getJobConfigs().forEach(jobConfig -> {
          LOG.debug("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
          LocalStreamProcessorLifeCycleListener listener = new LocalStreamProcessorLifeCycleListener();
          StreamProcessor processor = createStreamProcessor(jobConfig, ((StreamGraphSpec)streamApp.getGraph()).getOperatorSpecGraph(),
              streamApp.getContextManager(), listener);
          listener.setProcessor(processor);
          processors.add(processor);
        });
        numProcessorsToStart.set(processors.size());

        // 4. start the StreamProcessors
        streamApp.getProcessorLifecycleListner().beforeStart();
        processors.forEach(StreamProcessor::start);
        streamApp.getProcessorLifecycleListner().afterStart();
      } catch (Throwable throwable) {
        appStatus = ApplicationStatus.unsuccessfulFinish(throwable);
        shutdownLatch.countDown();
        throw new SamzaException(String.format("Failed to start application: %s.", streamApp), throwable);
      } finally {
        if (streamManager != null) {
          streamManager.stop();
        }
      }
    }

    @Override
    public void kill() {
      streamApp.getProcessorLifecycleListner().beforeStop();
      processors.forEach(StreamProcessor::stop);
      streamApp.getProcessorLifecycleListner().afterStop();
    }

    @Override
    public ApplicationStatus status() {
      return appStatus;
    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return LocalApplicationRunner.this.waitForFinish(timeout);
    }

  }

  private class TaskAppExecutable implements AppRuntimeExecutable {
    private final TaskAppSpecImpl appSpec;
    private StreamProcessor sp;

    private TaskAppExecutable(TaskAppSpecImpl appSpec) {
      this.appSpec = appSpec;
    }

    @Override
    public void run() {
      LOG.info("LocalApplicationRunner will start task " + appSpec.getGlobalAppId());
      LocalStreamProcessorLifeCycleListener listener = new LocalStreamProcessorLifeCycleListener();

      sp = createStreamProcessor(config, appSpec.getTaskFactory(), listener);

      numProcessorsToStart.set(1);
      listener.setProcessor(sp);
      appSpec.getProcessorLifecycleListner().beforeStart();
      sp.start();
      appSpec.getProcessorLifecycleListner().afterStart();
    }

    @Override
    public void kill() {
      appSpec.getProcessorLifecycleListner().beforeStop();
      sp.stop();
      appSpec.getProcessorLifecycleListner().afterStop();
    }

    @Override
    public ApplicationStatus status() {
      return appStatus;
    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return LocalApplicationRunner.this.waitForFinish(timeout);
    }

  }

  private boolean waitForFinish(Duration timeout) {
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
  private void createStreams(String planId,
      List<StreamSpec> intStreams,
      StreamManager streamManager) throws TimeoutException {
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
   * Create {@link StreamProcessor} based on the config
   * @param config config
   * @return {@link StreamProcessor]}
   */
  /* package private */
  StreamProcessor createStreamProcessor(
      Config config,
      TaskFactory taskFactory,
      StreamProcessorLifecycleListener listener) {
    return new StreamProcessor(config, this.metricsReporters, taskFactory, listener, null);
  }

  /**
   * Create {@link StreamProcessor} based on {@link OperatorSpecGraph}, {@link ContextManager} and the config
   * @param config config
   * @param graph {@link OperatorSpecGraph}
   * @param contextManager {@link ContextManager}
   * @return {@link StreamProcessor]}
   */
  /* package private */
  StreamProcessor createStreamProcessor(
      Config config,
      OperatorSpecGraph graph,
      ContextManager contextManager,
      StreamProcessorLifecycleListener listener) {
    TaskFactory taskFactory = () -> new StreamOperatorTask(graph, contextManager);
    return new StreamProcessor(config, this.metricsReporters, taskFactory, listener, null);
  }

  /* package private for testing */
  Set<StreamProcessor> getProcessors() {
    return processors;
  }

  @VisibleForTesting
  CountDownLatch getShutdownLatch() {
    return shutdownLatch;
  }

  @Override
  protected AppRuntimeExecutable getTaskAppRuntimeExecutable(TaskAppSpecImpl appSpec) {
    return new TaskAppExecutable(appSpec);
  }

  @Override
  protected AppRuntimeExecutable getStreamAppRuntimeExecutable(StreamAppSpecImpl appSpec) {
    return new StreamAppExecutable(appSpec);
  }

}
