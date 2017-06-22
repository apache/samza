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

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.apache.samza.coordinator.Latch;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.zk.ZkCoordinationServiceFactory;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link ApplicationRunner} that runs the applications in standalone environment
 */
public class LocalApplicationRunner extends AbstractApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(LocalApplicationRunner.class);
  // Latch id that's used for awaiting the init of application before creating the StreamProcessors
  private static final String INIT_LATCH_ID = "init";
  // Latch timeout is set to 10 min
  private static final long LATCH_TIMEOUT_MINUTES = 10;

  private final String uid;
  private final CoordinationUtils coordinationUtils;
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

      if (coordinationUtils != null) {
        coordinationUtils.reset();
      }
      shutdownLatch.countDown();
    }
  }

  public LocalApplicationRunner(Config config) {
    super(config);
    uid = UUID.randomUUID().toString();
    coordinationUtils = createCoordinationUtils();
  }

  @Override
  public void runTask() {
    JobConfig jobConfig = new JobConfig(this.config);

    // validation
    String taskName = new TaskConfig(config).getTaskClass().get();
    if (taskName == null) {
      throw new SamzaException("Neither APP nor task.class are defined defined");
    }
    LOG.info("LocalApplicationRunner will run " + taskName);
    LocalStreamProcessorLifeCycleListener listener = new LocalStreamProcessorLifeCycleListener();

    StreamProcessor processor = createStreamProcessor(jobConfig, null, listener);

    listener.setProcessor(processor);
    processor.start();
  }

  @Override
  public void run(StreamApplication app) {
    try {
      // 1. initialize and plan
      ExecutionPlan plan = getExecutionPlan(app);
      writePlanJsonFile(plan.getPlanAsJson());

      // 2. create the necessary streams
      createStreams(plan.getIntermediateStreams());

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
    } catch (Exception e) {
      throw new SamzaException("Failed to start application", e);
    }
  }

  @Override
  public void kill(StreamApplication streamApp) {
    processors.forEach(StreamProcessor::stop);
  }

  @Override
  public ApplicationStatus status(StreamApplication streamApp) {
    return appStatus;
  }

  /**
   * Block until the application finishes
   */
  public void waitForFinish() {
    try {
      shutdownLatch.await();
    } catch (Exception e) {
      LOG.error("Wait is interrupted by exception", e);
      throw new SamzaException(e);
    }
  }

  /**
   * Create the {@link CoordinationUtils} needed by the application runner, or null if it's not configured.
   * @return an instance of {@link CoordinationUtils}
   */
  /* package private */ CoordinationUtils createCoordinationUtils() {
    String jobCoordinatorFactoryClassName = config.get(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "");

    // TODO: we will need a better way to package the configs with application runner
    if (ZkJobCoordinatorFactory.class.getName().equals(jobCoordinatorFactoryClassName)) {
      ApplicationConfig appConfig = new ApplicationConfig(config);
      return new ZkCoordinationServiceFactory().getCoordinationService(appConfig.getGlobalAppId(), uid, config);
    } else {
      return null;
    }
  }

  /**
   * Create intermediate streams using {@link org.apache.samza.execution.StreamManager}.
   * If {@link CoordinationUtils} is provided, this function will first invoke leader election, and the leader
   * will create the streams. All the runner processes will wait on the latch that is released after the leader finishes
   * stream creation.
   * @param intStreams list of intermediate {@link StreamSpec}s
   * @throws Exception exception for latch timeout
   */
  /* package private */ void createStreams(List<StreamSpec> intStreams) throws Exception {
    if (!intStreams.isEmpty()) {
      if (coordinationUtils != null) {
        Latch initLatch = coordinationUtils.getLatch(1, INIT_LATCH_ID);
        LeaderElector leaderElector = coordinationUtils.getLeaderElector();
        leaderElector.setLeaderElectorListener(() -> {
            getStreamManager().createStreams(intStreams);
            initLatch.countDown();
          });
        leaderElector.tryBecomeLeader();
        initLatch.await(LATCH_TIMEOUT_MINUTES, TimeUnit.MINUTES);
      } else {
        // each application process will try creating the streams, which
        // requires stream creation to be idempotent
        getStreamManager().createStreams(intStreams);
      }
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
    Object taskFactory = TaskFactoryUtil.createTaskFactory(config, app, this);
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
}
