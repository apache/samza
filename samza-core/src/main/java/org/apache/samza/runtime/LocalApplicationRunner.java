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

import com.google.common.base.Objects;
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
import java.util.stream.Collectors;
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
  private static final String APPLICATION_RUNNER_ZK_PATH_SUFFIX = "/ApplicationRunnerData";
  // Latch timeout is set to 10 min
  private static final long LATCH_TIMEOUT_MINUTES = 10;
  private static final long LEADER_ELECTION_WAIT_TIME_MS = 1000;

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
      return new ZkCoordinationServiceFactory().getCoordinationService(
          appConfig.getGlobalAppId() + APPLICATION_RUNNER_ZK_PATH_SUFFIX, uid, config);
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
   * @throws TimeoutException exception for latch timeout
   */
  /* package private */ void createStreams(List<StreamSpec> intStreams) throws TimeoutException {
    if (!intStreams.isEmpty()) {
      // Move the scope of coordination utils within stream creation to address long idle connection problem.
      // Refer SAMZA-1385 for more details
      CoordinationUtils coordinationUtils = createCoordinationUtils();
      if (coordinationUtils != null) {
        // generate a unique reproducible id for an application lifecycle
        String latchId = generateLatchId(intStreams);
        Latch initLatch = coordinationUtils.getLatch(1, latchId);

        try {
          // check if the processor needs to go through leader election and stream creation
          if (shouldContestInElectionForStreamCreation(initLatch)) {
            LeaderElector leaderElector = coordinationUtils.getLeaderElector();
            leaderElector.setLeaderElectorListener(() -> {
                getStreamManager().createStreams(intStreams);
                initLatch.countDown();
              });
            leaderElector.tryBecomeLeader();
            initLatch.await(LATCH_TIMEOUT_MINUTES, TimeUnit.MINUTES);
          }
        } finally {
          coordinationUtils.reset();
        }
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

  /**
   * Generates a unique latch id that is consistent across processors within the same application lifecycle.
   * Each {@code StreamSpec} has an id that is unique and is used for hashcode computation.
   *
   * @param intStreams list of {@link StreamSpec}s
   * @return latch id
   */
  /* package private */
  String generateLatchId(List<StreamSpec> intStreams) {
    return String.valueOf(
        Objects.hashCode(
            intStreams.stream()
                .map(StreamSpec::getId)
                .sorted()
                .collect(Collectors.toList())));
  }

  /**
   * In order to fix SAMZA-1385, we are limiting the scope of coordination util within stream creation phase and destroying
   * the coordination util right after. By closing the zk connection, we clean up the ephemeral node used for leader election.
   * It creates the following issues whenever a new process joins after the ephemeral node is gone.
   *    1. It is unnecessary to re-conduct leader election for stream creation in the same application lifecycle
   *    2. Underlying systems may not support check for stream existence prior to creation which could have potential problems.
   * As in interim solution, we reuse the same latch as a marker to determine if create streams phase is done for the
   * application lifecycle using {@link Latch#await(long, TimeUnit)}
   *
   * @param streamCreationLatch latch used for stream creation
   * @return true if processor needs to be part of election
   *         false otherwise
   */
  private boolean shouldContestInElectionForStreamCreation(Latch streamCreationLatch) {
    boolean eligibleForElection = true;

    try {
      streamCreationLatch.await(LEADER_ELECTION_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
      // case we didn't time out suggesting that latch already exists
      eligibleForElection = false;
    } catch (TimeoutException e) {
      LOG.info("Timed out waiting for the latch! Going to enter leader election section to create streams");
    }

    return eligibleForElection;
  }
}
