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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainerListener;
import org.apache.samza.context.ApplicationContainerContext;
import org.apache.samza.context.ApplicationContainerContextFactory;
import org.apache.samza.context.ApplicationTaskContext;
import org.apache.samza.context.ApplicationTaskContextFactory;
import org.apache.samza.context.JobContextImpl;
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
import scala.Option;


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
 *                                                                                                   ────────────────────────────────
 *                                                                                                  │                               │
 *                                                                                                  │                               │
 *                                                                                                  │                               │
 *                                                                                                  │                               │
 *     New                                StreamProcessor.start()          Rebalance triggered      V        Receives JobModel      │
 *  StreamProcessor ──────────▶   NEW ───────────────────────────▶ STARTED ──────────────────▶ IN_REBALANCE ─────────────────────▶ RUNNING
 *   Creation                      │                                 │     by group leader          │     and starts Container      │
 *                                 │                                 │                              │                               │
 *                             Stre│amProcessor.stop()           Stre│amProcessor.stop()        Stre│amProcessor.stop()         Stre│amProcessor.stop()
 *                                 │                                 │                              │                               │
 *                                 │                                 │                              │                               │
 *                                 │                                 │                              │                               │
 *                                 V                                 V                              V                               V
 *                                  ───────────────────────────▶ STOPPING D──────────────────────────────────────────────────────────
 *                                                                  │
 *                                                                  │
 *                                            After JobCoordinator and SamzaContainer had shutdown.
 *                                                                  │
 *                                                                  V
 *                                                                 STOPPED
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
  /**
   * Type parameter needs to be {@link ApplicationContainerContext} so that we can eventually call the base methods of
   * the context object.
   */
  private final Optional<ApplicationContainerContextFactory<ApplicationContainerContext>> applicationDefinedContainerContextFactoryOptional;
  /**
   * Type parameter needs to be {@link ApplicationTaskContext} so that we can eventually call the base methods of the
   * context object.
   */
  private final Optional<ApplicationTaskContextFactory<ApplicationTaskContext>> applicationDefinedTaskContextFactoryOptional;
  private final Map<String, MetricsReporter> customMetricsReporter;
  private final Config config;
  private final long taskShutdownMs;
  private final String processorId;
  private final ExecutorService containerExcecutorService;
  private final Object lock = new Object();

  private volatile Throwable containerException = null;

  volatile CountDownLatch containerShutdownLatch = new CountDownLatch(1);

  /**
   * Indicates the current status of a {@link StreamProcessor}.
   */
  public enum State {
    STARTED("STARTED"), RUNNING("RUNNING"), STOPPING("STOPPING"), STOPPED("STOPPED"), NEW("NEW"), IN_REBALANCE("IN_REBALANCE");

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
    return state;
  }

  @VisibleForTesting
  State state = State.NEW;

  @VisibleForTesting
  SamzaContainer container = null;

  @VisibleForTesting
  JobCoordinatorListener jobCoordinatorListener = null;

  /**
   * Same as {@link #StreamProcessor(Config, Map, TaskFactory, ProcessorLifecycleListener, JobCoordinator)}, except
   * it creates a {@link JobCoordinator} instead of accepting it as an argument.
   *
   * @param config configuration required to launch {@link JobCoordinator} and {@link SamzaContainer}
   * @param customMetricsReporters registered with the metrics system to report metrics
   * @param taskFactory task factory to instantiate the Task
   * @param processorListener listener to the StreamProcessor life cycle
   *
   * Deprecated: Use {@link #StreamProcessor(Config, Map, TaskFactory, Optional, Optional,
   * StreamProcessorLifecycleListenerFactory, JobCoordinator)} instead.
   */
  @Deprecated
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters, TaskFactory taskFactory,
      ProcessorLifecycleListener processorListener) {
    this(config, customMetricsReporters, taskFactory, processorListener, null);
  }

  /**
   * Same as {@link #StreamProcessor(Config, Map, TaskFactory, Optional, Optional,
   * StreamProcessorLifecycleListenerFactory, JobCoordinator)}, with the following differences:
   * <ol>
   *   <li>Passes null for application-defined context factories</li>
   *   <li>Accepts a {@link ProcessorLifecycleListener} directly instead of a
   *   {@link StreamProcessorLifecycleListenerFactory}</li>
   * </ol>
   *
   * @param config configuration required to launch {@link JobCoordinator} and {@link SamzaContainer}
   * @param customMetricsReporters registered with the metrics system to report metrics
   * @param taskFactory task factory to instantiate the Task
   * @param processorListener listener to the StreamProcessor life cycle
   * @param jobCoordinator the instance of {@link JobCoordinator}
   *
   * Deprecated: Use {@link #StreamProcessor(Config, Map, TaskFactory, Optional, Optional,
   * StreamProcessorLifecycleListenerFactory, JobCoordinator)} instead.
   */
  @Deprecated
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters, TaskFactory taskFactory,
      ProcessorLifecycleListener processorListener, JobCoordinator jobCoordinator) {
    this(config, customMetricsReporters, taskFactory, Optional.empty(), Optional.empty(), sp -> processorListener,
        jobCoordinator);
  }

  /**
   * Builds a {@link StreamProcessor} with full specification of processing components.
   *
   * @param config configuration required to launch {@link JobCoordinator} and {@link SamzaContainer}
   * @param customMetricsReporters registered with the metrics system to report metrics
   * @param taskFactory task factory to instantiate the Task
   * @param applicationDefinedContainerContextFactoryOptional optional factory for application-defined container context
   * @param applicationDefinedTaskContextFactoryOptional optional factory for application-defined task context
   * @param listenerFactory factory for creating a listener to the StreamProcessor life cycle
   * @param jobCoordinator the instance of {@link JobCoordinator}
   */
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters, TaskFactory taskFactory,
      Optional<ApplicationContainerContextFactory<ApplicationContainerContext>> applicationDefinedContainerContextFactoryOptional,
      Optional<ApplicationTaskContextFactory<ApplicationTaskContext>> applicationDefinedTaskContextFactoryOptional,
      StreamProcessorLifecycleListenerFactory listenerFactory, JobCoordinator jobCoordinator) {
    Preconditions.checkNotNull(listenerFactory, "StreamProcessorListenerFactory cannot be null.");
    this.config = config;
    this.customMetricsReporter = customMetricsReporters;
    this.taskFactory = taskFactory;
    this.applicationDefinedContainerContextFactoryOptional = applicationDefinedContainerContextFactoryOptional;
    this.applicationDefinedTaskContextFactoryOptional = applicationDefinedTaskContextFactoryOptional;
    this.taskShutdownMs = new TaskConfigJava(config).getShutdownMs();
    this.jobCoordinator = (jobCoordinator != null) ? jobCoordinator : createJobCoordinator();
    this.jobCoordinatorListener = createJobCoordinatorListener();
    this.jobCoordinator.setListener(jobCoordinatorListener);
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(CONTAINER_THREAD_NAME_FORMAT).setDaemon(true).build();
    this.containerExcecutorService = Executors.newSingleThreadExecutor(threadFactory);
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
    synchronized (lock) {
      if (state == State.NEW) {
        processorListener.beforeStart();
        state = State.STARTED;
        jobCoordinator.start();
      } else {
        LOGGER.info("Start is no-op, since the current state is {} and not {}.", state, State.NEW);
      }
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
    synchronized (lock) {
      if (state != State.STOPPING && state != State.STOPPED) {
        state = State.STOPPING;
        try {
          LOGGER.info("Shutting down the container: {} of stream processor: {}.", container, processorId);
          boolean hasContainerShutdown = stopSamzaContainer();
          if (!hasContainerShutdown) {
            LOGGER.info("Interrupting the container: {} thread to die.", container);
            containerExcecutorService.shutdownNow();
          }
        } catch (Throwable throwable) {
          LOGGER.error(String.format("Exception occurred on container: %s shutdown of stream processor: %s.", container, processorId), throwable);
        }
        LOGGER.info("Shutting down JobCoordinator of stream processor: {}.", processorId);
        jobCoordinator.stop();
      } else {
        LOGGER.info("StreamProcessor state is: {}. Ignoring the stop.", state);
      }
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
    return SamzaContainer.apply(processorId, jobModel, ScalaJavaUtil.toScalaMap(this.customMetricsReporter),
        this.taskFactory, JobContextImpl.fromConfigWithDefaults(this.config),
        Option.apply(this.applicationDefinedContainerContextFactoryOptional.orElse(null)),
        Option.apply(this.applicationDefinedTaskContextFactoryOptional.orElse(null)));
  }

  private JobCoordinator createJobCoordinator() {
    String jobCoordinatorFactoryClassName = new JobCoordinatorConfig(config).getJobCoordinatorFactoryClassName();
    return Util.getObj(jobCoordinatorFactoryClassName, JobCoordinatorFactory.class).getJobCoordinator(config);
  }

  /**
   * Stops the {@link SamzaContainer}.
   * @return true if {@link SamzaContainer} had shutdown within task.shutdown.ms. false otherwise.
   */
  private boolean stopSamzaContainer() {
    boolean hasContainerShutdown = true;
    if (container != null) {
      try {
        container.shutdown();
        LOGGER.info("Waiting {} ms for the container: {} to shutdown.", taskShutdownMs, container);
        hasContainerShutdown = containerShutdownLatch.await(taskShutdownMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOGGER.error("Exception occurred when shutting down the container: {}.", container, e);
        hasContainerShutdown = false;
        if (containerException != null) {
          containerException = e;
        }
      }
      LOGGER.info(String.format("Shutdown status of container: %s for stream processor: %s is: %b.", container, processorId, hasContainerShutdown));
    }

    // We want to propagate TimeoutException when container shutdown times out. It is possible that the timeout exception
    // we propagate to the application runner maybe overwritten by container failure cause in case of interleaved execution.
    // It is acceptable since container exception is much more useful compared to timeout exception.
    // We can infer from the logs about the fact that container shutdown timed out or not for additional inference.
    if (!hasContainerShutdown && containerException == null) {
      containerException = new TimeoutException("Container shutdown timed out after " + taskShutdownMs + " ms.");
    }

    return hasContainerShutdown;
  }

  private JobCoordinatorListener createJobCoordinatorListener() {
    return new JobCoordinatorListener() {

      @Override
      public void onJobModelExpired() {
        synchronized (lock) {
          if (state == State.STARTED || state == State.RUNNING) {
            state = State.IN_REBALANCE;
            LOGGER.info("Job model expired. Shutting down the container: {} of stream processor: {}.", container, processorId);
            boolean hasContainerShutdown = stopSamzaContainer();
            if (!hasContainerShutdown) {
              LOGGER.warn("Container: {} shutdown was unsuccessful. Stopping the stream processor: {}.", container, processorId);
              state = State.STOPPING;
              jobCoordinator.stop();
            } else {
              LOGGER.info("Container: {} shutdown completed for stream processor: {}.", container, processorId);
            }
          } else {
            LOGGER.info("Ignoring onJobModelExpired invocation since the current state is {} and not in {}.", state, ImmutableList.of(State.RUNNING, State.STARTED));
          }
        }
      }

      @Override
      public void onNewJobModel(String processorId, JobModel jobModel) {
        synchronized (lock) {
          if (state == State.IN_REBALANCE) {
            containerShutdownLatch = new CountDownLatch(1);
            container = createSamzaContainer(processorId, jobModel);
            container.setContainerListener(new ContainerListener());
            LOGGER.info("Starting the container: {} for the stream processor: {}.", container, processorId);
            containerExcecutorService.submit(container);
          } else {
            LOGGER.info("Ignoring onNewJobModel invocation since the current state is {} and not {}.", state, State.IN_REBALANCE);
          }
        }
      }

      @Override
      public void onCoordinatorStop() {
        synchronized (lock) {
          LOGGER.info("Shutting down the executor service of the stream processor: {}.", processorId);
          boolean hasContainerShutdown = stopSamzaContainer();

          // we only want to interrupt when container shutdown times out.
          if (!hasContainerShutdown) {
            containerExcecutorService.shutdownNow();
          }
          state = State.STOPPED;
        }
        if (containerException != null)
          processorListener.afterFailure(containerException);
        else
          processorListener.afterStop();

      }

      @Override
      public void onCoordinatorFailure(Throwable throwable) {
        synchronized (lock) {
          LOGGER.info(String.format("Coordinator: %s failed with an exception. Stopping the stream processor: %s. Original exception:", jobCoordinator, processorId), throwable);
          boolean hasContainerShutdown = stopSamzaContainer();

          // we only want to interrupt when container shutdown times out.
          if (!hasContainerShutdown) {
            containerExcecutorService.shutdownNow();
          }
          state = State.STOPPED;
        }
        processorListener.afterFailure(throwable);
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
      LOGGER.warn("Received container start notification for container: {} in stream processor: {}.", container, processorId);
      if (!processorOnStartCalled) {
        processorListener.afterStart();
        processorOnStartCalled = true;
      }
      state = State.RUNNING;
    }

    @Override
    public void afterStop() {
      containerShutdownLatch.countDown();

      synchronized (lock) {
        if (state == State.IN_REBALANCE) {
          LOGGER.info("Container: {} of the stream processor: {} was stopped by the JobCoordinator.", container, processorId);
        } else {
          LOGGER.info("Container: {} stopped. Stopping the stream processor: {}.", container, processorId);
          state = State.STOPPING;
          jobCoordinator.stop();
        }
      }
    }

    @Override
    public void afterFailure(Throwable t) {
      containerException = t;
      containerShutdownLatch.countDown();

      synchronized (lock) {
        LOGGER.error(String.format("Container: %s failed with an exception. Stopping the stream processor: %s. Original exception:", container, processorId), t);
        state = State.STOPPING;
        jobCoordinator.stop();
      }
    }
  }
}
