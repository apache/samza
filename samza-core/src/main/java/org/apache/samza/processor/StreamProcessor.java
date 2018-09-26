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
package org.apache.samza.processor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.IllegalContainerStateException;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainerListener;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.runtime.ProcessorLifecycleListener;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.util.ScalaJavaUtil;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.processor.StreamProcessor.State.*;
import static org.apache.samza.util.StateTransitionUtil.*;

/**
 * StreamProcessor can be embedded in any application or executed in a distributed environment (aka cluster) as an
 * independent process.
 *
 * <p>
 *
 * <b>Note</b>: A single JVM can create multiple StreamProcessor instances. It is safe to create StreamProcessor instances in
 * multiple threads. This class is thread safe.
 *
 * </p>
 *
 * <pre>
 * A StreamProcessor could be in any one of the following states:
 * NEW, STARTED, IN_REBALANCE, RUNNING, STOPPING, STOPPED.
 *
 * Describes the valid state transitions of the {@link StreamProcessor}.
 *
 *
 *                                                                                                                                                           ───────────────────────────────
 *                                                                                                                                                          │                               │
 *                                                                                                                                                          │                               │
 *                                                                                                                                                          │                               │
 *                                                                                                                                                          │                               │
 *     New                                StreamProcessor.start()                 Rebalance triggered                                                       V        Receives JobModel      │
 *  StreamProcessor ──────────▶   NEW ───────────────────────────▶ STARTED ─────────────────–––––––––––––––––––––─▶ START_REBALANCE ––––––––––––––––▶ IN_REBALANCE ─────────────────────▶ RUNNING
 *   Creation                      │                                 │     by group leader and starts Container          │                                                                  │
 *                                 │                                 │                                                   │                                                                  │
 *                             Stre│amProcessor.stop()           Stre│amProcessor.stop()                             Stre│amProcessor.stop()                                            Stre│amProcessor.stop()
 *                                 │                                 │                                                   │                                                                  │
 *                                 │                                 │                                                   │                                                                  │
 *                                 │                                 │                                                   │                                                                  │
 *                                 V                                 V                                                   V                                                                  V
 *                                  ───────────–––––––––––––––––––––––––––––––––––––––––––––──────────────––––––––──▶ STOPPING ──────────────────────────────────────────────────────────–––
 *                                                                                                                       │
 *                                                                                                                       │
 *                                                                                                  After JobCoordinator and SamzaContainer had shutdown.
 *                                                                                                                       │
 *                                                                                                                       V
 *                                                                                                                    STOPPED
 *
 * </pre>
 */
@InterfaceStability.Evolving
public class StreamProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProcessor.class);
  private static final String CONTAINER_THREAD_NAME_FORMAT = "Samza StreamProcessor Container Thread-%d";

  private final JobCoordinator jobCoordinator;
  private final ProcessorLifecycleListener processorListener;
  private final TaskFactory taskFactory;
  private final Map<String, MetricsReporter> customMetricsReporter;
  private final Config config;
  private final long taskShutdownMs;
  private final String processorId;
  private final ExecutorService executorService;

  private Throwable containerException = null;

  @VisibleForTesting
  AtomicReference<State> state = new AtomicReference<>(NEW);

  @VisibleForTesting
  SamzaContainer container = null;

  @VisibleForTesting
  JobCoordinatorListener jobCoordinatorListener = null;

  volatile CountDownLatch containerShutdownLatch = new CountDownLatch(0);

  /**
   * Indicates the current status of a {@link StreamProcessor}.
   */
  public enum State {
    STARTED("STARTED"), RUNNING("RUNNING"), STOPPING("STOPPING"), STOPPED("STOPPED"), NEW("NEW"),
    START_REBALANCE("START_REBALANCE"), IN_REBALANCE("IN_REBALANCE");

    private String strVal;

    State(String strVal) {
      this.strVal = strVal;
    }

    @Override
    public String toString() {
      return strVal;
    }
  }

  /**
   * @return the current state of StreamProcessor.
   */
  public State getState() {
    return state.get();
  }

  /**
   * StreamProcessor encapsulates and manages the lifecycle of {@link JobCoordinator} and {@link SamzaContainer}.
   *
   * <p>
   * On startup, StreamProcessor starts the JobCoordinator. Schedules the SamzaContainer to run in a ExecutorService
   * when it receives new {@link JobModel} from JobCoordinator.
   * <p>
   *
   * <b>Note:</b> Lifecycle of the ExecutorService is fully managed by the StreamProcessor.
   *
   * @param config configuration required to launch {@link JobCoordinator} and {@link SamzaContainer}.
   * @param customMetricsReporters metricReporter instances that will be used by SamzaContainer and JobCoordinator to report metrics.
   * @param taskFactory the {@link TaskFactory} to be used for creating task instances.
   * @param processorListener listener to the StreamProcessor life cycle.
   */
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters, TaskFactory taskFactory,
      ProcessorLifecycleListener processorListener) {
    this(config, customMetricsReporters, taskFactory, processorListener, null);
  }

  /**
   * Same as {@link #StreamProcessor(Config, Map, TaskFactory, ProcessorLifecycleListener)}, except the
   * {@link JobCoordinator} is given for this {@link StreamProcessor}.
   * @param config configuration required to launch {@link JobCoordinator} and {@link SamzaContainer}
   * @param customMetricsReporters metric Reporter
   * @param taskFactory task factory to instantiate the Task
   * @param processorListener listener to the StreamProcessor life cycle
   * @param jobCoordinator the instance of {@link JobCoordinator}
   */
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters, TaskFactory taskFactory,
      ProcessorLifecycleListener processorListener, JobCoordinator jobCoordinator) {
    this(config, customMetricsReporters, taskFactory, sp -> processorListener, jobCoordinator);
  }

  /**
   * Same as {@link #StreamProcessor(Config, Map, TaskFactory, ProcessorLifecycleListener, JobCoordinator)}, except
   * there is a {@link StreamProcessorLifecycleListenerFactory} as input instead of {@link ProcessorLifecycleListener}.
   * This is useful to create a {@link ProcessorLifecycleListener} with a reference to this {@link StreamProcessor}
   *
   * @param config configuration required to launch {@link JobCoordinator} and {@link SamzaContainer}
   * @param customMetricsReporters metric Reporter
   * @param taskFactory task factory to instantiate the Task
   * @param listenerFactory listener to the StreamProcessor life cycle
   * @param jobCoordinator the instance of {@link JobCoordinator}
   */
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters, TaskFactory taskFactory,
      StreamProcessorLifecycleListenerFactory listenerFactory, JobCoordinator jobCoordinator) {
    Preconditions.checkNotNull(listenerFactory, "StreamProcessorListenerFactory cannot be null.");
    this.taskFactory = taskFactory;
    this.config = config;
    this.taskShutdownMs = new TaskConfigJava(config).getShutdownMs();
    this.customMetricsReporter = customMetricsReporters;
    this.jobCoordinator = (jobCoordinator != null) ? jobCoordinator : createJobCoordinator();
    this.jobCoordinatorListener = createJobCoordinatorListener();
    this.jobCoordinator.setListener(jobCoordinatorListener);
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(CONTAINER_THREAD_NAME_FORMAT).setDaemon(true).build();
    this.executorService = Executors.newSingleThreadExecutor(threadFactory);
    // TODO: remove the dependency on jobCoordinator for processorId after fixing SAMZA-1835
    this.processorId = this.jobCoordinator.getProcessorId();
    this.processorListener = listenerFactory.createInstance(this);
  }

  /**
   * Asynchronously starts this {@link StreamProcessor}.
   * <p>
   *   <b>Implementation</b>:
   *   Starts the {@link JobCoordinator}, which will eventually start the {@link SamzaContainer} when a new
   *   {@link JobModel} is available.
   * </p>
   */
  public void start() {
    if (state.get() == NEW) {
      processorListener.beforeStart();
      jobCoordinator.start();

      if (!state.compareAndSet(NEW, STARTED)) {
        LOGGER.info("Failed to transition to {} from {}Stream processor has already started and the current state {}", state.get());
        // todo: Should stop be invoked or do we throw exception?
        // todo: We ideally want to call stop to make sure resources spun by jobcoordinator.start() are cleaned up
      }
    } else {
      LOGGER.info("Stream processor has already been initialized and the current state {}", state.get());
    }
  }

  /**
   * <p>
   * Asynchronously stops the {@link StreamProcessor}'s running components - {@link SamzaContainer}
   * and {@link JobCoordinator}
   * </p>
   * Here're the ways which can stop the StreamProcessor:
   * <ol>
   *   <li>Caller of StreamProcessor invokes stop()</li>
   *   <li>Samza Container completes processing (eg. bounded input) and shuts down</li>
   *   <li>Samza Container fails</li>
   *   <li>Job Coordinator fails</li>
   * </ol>
   * When either container or coordinator stops (cleanly or due to exception), it will try to shutdown the
   * StreamProcessor. This needs to be synchronized so that only one code path gets triggered for shutdown.
   * <br>
   * If container is running,
   * <ol>
   *   <li>container is shutdown cleanly and {@link SamzaContainerListener#afterStop()} will trigger
   *   {@link JobCoordinator#stop()}</li>
   *   <li>container fails to shutdown cleanly and {@link SamzaContainerListener#afterFailure(Throwable)} will
   *   trigger {@link JobCoordinator#stop()}</li>
   * </ol>
   * If container is not running, then this method will simply shutdown the {@link JobCoordinator}.
   *
   */
  public void stop() {
    if (compareNotInAndSet(state, ImmutableSet.of(STOPPING, STOPPED), STOPPING)) {
      LOGGER.info("Shutting down stream processor: {}", processorId);
      stopSamzaContainer();

      LOGGER.info("Shutting down job coordinator for the stream processor: {}", processorId);
      jobCoordinator.stop();
    } else if (state.get() == STOPPING || state.get() == STOPPED) {
      LOGGER.info("Shutdown in progress for stream processor: {} with state {}", processorId, state.get());
    }
  }

  @VisibleForTesting
  JobCoordinator getCurrentJobCoordinator() {
    return jobCoordinator;
  }

  @VisibleForTesting
  SamzaContainer getContainer() {
    return container;
  }

  @VisibleForTesting
  SamzaContainer createSamzaContainer(String processorId, JobModel jobModel) {
    return SamzaContainer.apply(processorId, jobModel, config, ScalaJavaUtil.toScalaMap(customMetricsReporter), taskFactory);
  }

  private JobCoordinator createJobCoordinator() {
    String jobCoordinatorFactoryClassName = new JobCoordinatorConfig(config).getJobCoordinatorFactoryClassName();
    return Util.getObj(jobCoordinatorFactoryClassName, JobCoordinatorFactory.class).getJobCoordinator(config);
  }

  /**
   * Stops the {@link SamzaContainer}.
   */
  private void stopSamzaContainer() {
    LOGGER.info("Shutting down the container: {} for the stream processor: {}", container, processorId);
    if (container != null) {
      if (!container.hasStopped()) {
        try {
          container.shutdown();
        } catch (IllegalContainerStateException icse) {
          LOGGER.info(String.format("Cannot shutdown container: %s for stream processor: %s. Container is not running.", container, processorId), icse);
        }
      } else {
        LOGGER.info("Container is not instantiated for the stream processor: {}", processorId);
      }
    }
  }

  private JobCoordinatorListener createJobCoordinatorListener() {
    return new JobCoordinatorListener() {

      @Override
      public void onJobModelExpired() {
        if (compareAndSet(state, ImmutableSet.of(STARTED, RUNNING), START_REBALANCE)) {
          LOGGER.info("Job model expired for the stream processor: {}", processorId);
          stopSamzaContainer();

          LOGGER.info("Entering re-balance phase with a barrier on container shutdown for the stream processor: {}", processorId);
          boolean inRebalance = compareAndSetWithBarrier(state, START_REBALANCE
              , IN_REBALANCE, containerShutdownLatch, Duration.ofMillis(taskShutdownMs));

          // failed to transition to IN_REBALANCE either container shutdown failed or barrier timed out
          if (!inRebalance) {
            LOGGER.warn("Failed to transition to re-balance phase."
                + " Stopping the stream processor: {} due to unclean container shutdown.", processorId);
            jobCoordinator.stop();
          } else {
            LOGGER.info("Successfully transitioned to re-balance phase for the stream processor: {}", processorId);
          }
        } else {
          LOGGER.info("Ignoring onJobModelExpired since the current state is {} and not in {}.",
              state.get(), ImmutableSet.of(RUNNING, STARTED));
        }
      }

      @Override
      public void onNewJobModel(String processorId, JobModel jobModel) {
        if (state.get() == IN_REBALANCE) {
          LOGGER.info("New job model received for the stream processor: {}", processorId);
          containerShutdownLatch = new CountDownLatch(1);
          container = createSamzaContainer(processorId, jobModel);
          container.setContainerListener(new ContainerListener());
          LOGGER.info("Starting the container: {} for the stream processor: {}.", container, processorId);
          executorService.submit(container);
        } else {
          LOGGER.info("Ignoring onNewJobModel invocation since the current state is {} and not {}.",
              state.get(), State.IN_REBALANCE);
        }
      }

      @Override
      public void onCoordinatorStop() {
        LOGGER.info("Received shutdown request from job coordinator. Shutting down the stream processor: {}", processorId);

        // Stop samza container only when stream processor state is not in STOPPING or STOPPED
        if (compareNotInAndSet(state, ImmutableSet.of(STOPPING, STOPPED), STOPPING)) {
          stopSamzaContainer();
        }

        if (state.get() == STOPPING) {
          LOGGER.info("Attempting to shutdown stream processor: {} with a barrier on container shutdown.", processorId);
          boolean stopped = compareAndSetWithBarrier(state, STOPPING, STOPPED, containerShutdownLatch, Duration.ofMillis(taskShutdownMs));

          if (containerException != null) {
            processorListener.afterFailure(containerException);
          } else if (stopped) {
            processorListener.afterStop();
          } else {
            executorService.shutdownNow();
            processorListener.afterFailure(new SamzaException("Samza container did not shutdown cleanly."));
          }

          LOGGER.info("Shutdown for the stream processor: {} completed with status={}"
              , processorId, containerException == null && stopped);
        } else {
          LOGGER.info("Ignoring shutdown request since the current state is {} and not {} for the stream processor: {}"
              , new Object[] {state.get(), STOPPING, processorId});
        }
      }

      @Override
      public void onCoordinatorFailure(Throwable throwable) {
        LOGGER.info("Received shutdown request for the stream processor: {} from job coordinator due to {}"
            , processorId, throwable);

        if (compareNotInAndSet(state, ImmutableSet.of(STOPPING, STOPPED), STOPPING)) {
          stopSamzaContainer();
        }

        if (state.get() == STOPPING) {
          LOGGER.info("Attempting to shutdown stream processor: {} with a barrier on container shutdown.", processorId);
          if (!compareAndSetWithBarrier(state, STOPPING, STOPPED, containerShutdownLatch, Duration.ofMillis(taskShutdownMs))) {
            executorService.shutdownNow();
          }

          // todo: Should we prioritize which exception to throw to the processor listener?
          processorListener.afterFailure(throwable);
          LOGGER.info("Shutdown for the stream processor: {} completed with status=false");
        } else {
          LOGGER.info("Ignoring shutdown request since the current state is {} and not {} for the stream processor: {}"
              , new Object[] {state.get(), STOPPING, processorId});
        }
      }
    };
  }

  /**
   * Interface to create a {@link ProcessorLifecycleListener}
   */
  @FunctionalInterface
  public interface StreamProcessorLifecycleListenerFactory {
    ProcessorLifecycleListener createInstance(StreamProcessor processor);
  }

  class ContainerListener implements SamzaContainerListener {

    private boolean processorOnStartCalled = false;

    @Override
    public void beforeStart() {
      // processorListener.beforeStart() is invoked in StreamProcessor.start()
    }

    @Override
    public void afterStart() {
      LOGGER.info("Received container start notification from container: {} for stream processor: {}",
          container, processorId);

      if (compareAndSet(state, ImmutableSet.of(STARTED, IN_REBALANCE), RUNNING)) {
        if (!processorOnStartCalled) {
          processorListener.afterStart();
          processorOnStartCalled = true;
        }
        LOGGER.info("Stream processor started!");
      } else {
        LOGGER.info("Invalid state transition from {} to {}", state.get(), RUNNING);
      }
    }

    @Override
    public void afterStop() {
      LOGGER.info("Received stop notification from container: {} for stream processor: {}", container, processorId);
      containerShutdownLatch.countDown();

      if (compareNotInAndSet(state, ImmutableSet.of(STOPPING, STOPPED, START_REBALANCE, IN_REBALANCE), STOPPING)) {
        LOGGER.info("Container: {} stopped. Stopping the stream processor: {}.", container, processorId);
        jobCoordinator.stop();
      }
    }

    @Override
    public void afterFailure(Throwable t) {
      LOGGER.info("Received failure notification from container: {} for stream processor: {}", container, processorId);
      containerException = t;
      containerShutdownLatch.countDown();

      if (compareNotInAndSet(state, ImmutableSet.of(STOPPING, STOPPED), STOPPING)) {
        LOGGER.error("Stopping the stream processor: {} due to container exception {}", processorId, t);
        jobCoordinator.stop();
      }
    }
  }
}
