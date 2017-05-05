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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.samza.SamzaContainerStatus;
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
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * StreamProcessor can be embedded in any application or executed in a distributed environment (aka cluster) as an
 * independent process.
 * <p>
 *
 * <b>Note</b>: A single JVM can create multiple StreamProcessor instances. It is safe to create StreamProcessor instances in
 * multiple threads.
 */
@InterfaceStability.Evolving
public class StreamProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProcessor.class);

  private final JobCoordinator jobCoordinator;
  private final StreamProcessorLifecycleListener processorListener;
  private final Object taskFactory;
  private final Map<String, MetricsReporter> customMetricsReporter;
  private final Config config;
  private final long taskShutdownMs;

  private ExecutorService executorService;

  private volatile SamzaContainer container = null;
  // Latch used to synchronize between the JobCoordinator thread and the container thread, when the container is
  // stopped due to re-balancing
  private volatile CountDownLatch jcContainerShutdownLatch = new CountDownLatch(1);
  private volatile boolean processorOnStartCalled = false;

  @VisibleForTesting
  JobCoordinatorListener jobCoordinatorListener = null;

  /**
   * Create an instance of StreamProcessor that encapsulates a JobCoordinator and Samza Container
   * <p>
   * JobCoordinator controls how the various StreamProcessor instances belonging to a job coordinate. It is also
   * responsible generating and updating JobModel.
   * When StreamProcessor starts, it starts the JobCoordinator and brings up a SamzaContainer based on the JobModel.
   * SamzaContainer is executed using an ExecutorService.
   * <p>
   * <b>Note:</b> Lifecycle of the ExecutorService is fully managed by the StreamProcessor, and NOT exposed to the user
   *
   * @param config                 Instance of config object - contains all configuration required for processing
   * @param customMetricsReporters Map of custom MetricReporter instances that are to be injected in the Samza job
   * @param asyncStreamTaskFactory The {@link AsyncStreamTaskFactory} to be used for creating task instances.
   * @param processorListener         listener to the StreamProcessor life cycle
   */
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters,
                         AsyncStreamTaskFactory asyncStreamTaskFactory, StreamProcessorLifecycleListener processorListener) {
    this(config, customMetricsReporters, (Object) asyncStreamTaskFactory, processorListener);
  }

  /**
   *Same as {@link #StreamProcessor(Config, Map, AsyncStreamTaskFactory, StreamProcessorLifecycleListener)}, except task
   * instances are created using the provided {@link StreamTaskFactory}.
   * @param config - config
   * @param customMetricsReporters metric Reporter
   * @param streamTaskFactory task factory to instantiate the Task
   * @param processorListener  listener to the StreamProcessor life cycle
   */
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters,
                         StreamTaskFactory streamTaskFactory, StreamProcessorLifecycleListener processorListener) {
    this(config, customMetricsReporters, (Object) streamTaskFactory, processorListener);
  }

  /* package private */
  JobCoordinator getJobCoordinator() {
    return Util.
        <JobCoordinatorFactory>getObj(
            new JobCoordinatorConfig(config)
                .getJobCoordinatorFactoryClassName())
        .getJobCoordinator(config);
  }

  @VisibleForTesting
  StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters, Object taskFactory,
                  StreamProcessorLifecycleListener processorListener, JobCoordinator jobCoordinator) {
    this.taskFactory = taskFactory;
    this.config = config;
    this.taskShutdownMs = new TaskConfigJava(config).getShutdownMs();
    this.customMetricsReporter = customMetricsReporters;
    this.processorListener = processorListener;
    this.jobCoordinator = jobCoordinator;
    this.jobCoordinatorListener = createJobCoordinatorListener();
    this.jobCoordinator.setListener(jobCoordinatorListener);
  }

  private StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters,
                          Object taskFactory, StreamProcessorLifecycleListener processorListener) {
    this.taskFactory = taskFactory;
    this.config = config;
    this.taskShutdownMs = new TaskConfigJava(config).getShutdownMs();
    this.customMetricsReporter = customMetricsReporters;
    this.processorListener = processorListener;
    this.jobCoordinator = getJobCoordinator();
    this.jobCoordinator.setListener(createJobCoordinatorListener());
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
    jobCoordinator.start();
  }

  /**
   * <p>
   * Asynchronously stops the {@link StreamProcessor}'s running components - {@link SamzaContainer}
   * and {@link JobCoordinator}
   * </p>
   * There are multiple ways in which the StreamProcessor stops:
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
   *   <li>container is shutdown cleanly and {@link SamzaContainerListener#onContainerStop(boolean)} will trigger
   *   {@link JobCoordinator#stop()}</li>
   *   <li>container fails to shutdown cleanly and {@link SamzaContainerListener#onContainerFailed(Throwable)} will
   *   trigger {@link JobCoordinator#stop()}</li>
   * </ol>
   * If container is not running, then this method will simply shutdown the {@link JobCoordinator}.
   *
   */
  public synchronized void stop() {
    boolean containerShutdownInvoked = false;
    if (container != null) {
      try {
        LOGGER.info("Shutting down container " + container.toString() + " from StreamProcessor");
        container.shutdown();
        containerShutdownInvoked = true;
      } catch (IllegalContainerStateException icse) {
        LOGGER.info("Container was not running", icse);
      }
    }

    if (!containerShutdownInvoked) {
      LOGGER.info("Shutting down JobCoordinator from StreamProcessor");
      jobCoordinator.stop();
    }

  }

  SamzaContainer createSamzaContainer(ContainerModel containerModel, int maxChangelogStreamPartitions) {
    return SamzaContainer.apply(
        containerModel,
        config,
        maxChangelogStreamPartitions,
        Util.<String, MetricsReporter>javaMapAsScalaMap(customMetricsReporter),
        taskFactory);
  }

  JobCoordinatorListener createJobCoordinatorListener() {
    return new JobCoordinatorListener() {

      @Override
      public void onJobModelExpired() {
        if (container != null) {
          SamzaContainerStatus status = container.getStatus();
          if (SamzaContainerStatus.NOT_STARTED.equals(status) || SamzaContainerStatus.STARTED.equals(status)) {
            boolean shutdownComplete = false;
            try {
              LOGGER.info("Shutting down container in onJobModelExpired.");
              container.pause();
              shutdownComplete = jcContainerShutdownLatch.await(taskShutdownMs, TimeUnit.MILLISECONDS);
            } catch (IllegalContainerStateException icse) {
              // Ignored since container is not running
              LOGGER.info("Container was not running.", icse);
              shutdownComplete = true;
            } catch (InterruptedException e) {
              LOGGER.warn("Container shutdown was interrupted!" + container.toString(), e);
            }
            if (!shutdownComplete) {
              LOGGER.warn("Container " + container.toString() + " may not have shutdown successfully. " +
                  "Stopping the processor.");
              container = null;
              stop();
            } else {
              LOGGER.debug("Container " + container.toString() + " shutdown successfully");
            }
          } else {
            LOGGER.debug("Container " + container.toString() + " is not running.");
          }
        } else {
          LOGGER.debug("Container is not instantiated yet.");
        }
      }

      @Override
      public void onNewJobModel(String processorId, JobModel jobModel) {
        if (!jobModel.getContainers().containsKey(processorId)) {
          LOGGER.warn("JobModel does not contain the processorId: " + processorId + ". Stopping the processor.");
          stop();
        } else {
          jcContainerShutdownLatch = new CountDownLatch(1);

          SamzaContainerListener containerListener = new SamzaContainerListener() {
            @Override
            public void onContainerStart() {
              if (!processorOnStartCalled) {
                // processorListener is called on start only the first time the container starts.
                // It is not called after every re-balance of partitions among the processors
                processorOnStartCalled = true;
                if (processorListener != null) {
                  processorListener.onStart();
                }
              } else {
                LOGGER.debug("StreamProcessorListener was notified of container start previously. Hence, skipping this time.");
              }
            }

            @Override
            public void onContainerStop(boolean pauseByJm) {
              if (pauseByJm) {
                LOGGER.info("Container " + container.toString() + " stopped due to a request from JobCoordinator.");
                if (jcContainerShutdownLatch != null) {
                  jcContainerShutdownLatch.countDown();
                }
              } else {  // sp.stop was called or container stopped by itself
                LOGGER.info("Container " + container.toString() + " stopped.");
                container = null; // this guarantees that stop() doesn't try to stop container again
                stop();
              }
            }

            @Override
            public void onContainerFailed(Throwable t) {
              if (jcContainerShutdownLatch != null) {
                jcContainerShutdownLatch.countDown();
              } else {
                LOGGER.warn("JobCoordinatorLatch was null. It is possible for some component to be waiting.");
              }
              LOGGER.error("Container failed. Stopping the processor.", t);
              container = null;
              stop();
            }
          };

          container = createSamzaContainer(
              jobModel.getContainers().get(processorId),
              jobModel.maxChangeLogStreamPartitions);
          container.setContainerListener(containerListener);
          LOGGER.info("Starting container " + container.toString());
          executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
              .setNameFormat("p-" + processorId + "-container-thread-%d").build());
          executorService.submit(container::run);
        }
      }

      @Override
      public void onCoordinatorStop() {
        if (executorService != null) {
          LOGGER.info("Shutting down the executor service.");
          executorService.shutdownNow();
        }
        if (processorListener != null) {
          processorListener.onShutdown();
        }
      }

      @Override
      public void onCoordinatorFailure(Throwable e) {
        LOGGER.info("Coordinator Failed. Stopping the processor.");
        stop();
        if (processorListener != null) {
          processorListener.onFailure(e);
        }
      }
    };
  }
}
