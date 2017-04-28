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
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.JmxServer;
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
 * <p>
 *   StreamProcessor can be embedded in any application or executed in a distributed environment (aka cluster) as an
 *   independent process.
 * </p>
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

  @VisibleForTesting
  JobCoordinatorListener jobCoordinatorListener = null;

  // Latch used to synchronize between the JobCoordinator thread and the container thread, when the container is
  // stopped due to re-balancing
  private volatile CountDownLatch jcContainerLatch = new CountDownLatch(1);
  private volatile boolean processorOnStartCalled = false;

  SamzaContainer createSamzaContainer(ContainerModel containerModel, int maxChangelogStreamPartitions, JmxServer jmxServer) {
    return SamzaContainer.apply(
        containerModel,
        config,
        maxChangelogStreamPartitions,
        jmxServer,
        Util.<String, MetricsReporter>javaMapAsScalaMap(customMetricsReporter),
        taskFactory);
  }

  JobCoordinatorListener createJobCoordinatorListener() {
    return new JobCoordinatorListener() {

      @Override
      public void onJobModelExpired() {
        if (container != null && container.getStatus().equals(SamzaContainerStatus.RUNNING)) {
          container.shutdown(true);
          boolean shutdownComplete = false;
          try {
            shutdownComplete = jcContainerLatch.await(taskShutdownMs, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            LOGGER.warn("Container shutdown was interrupted!" + container.toString());
          }
          if (!shutdownComplete) {
            LOGGER.warn("Container " + container.toString() + " may not have shutdown successfully. Stopping the processor.");
            container = null;
            stop();
          } else {
            LOGGER.info("Container " + container.toString() + " shutdown successfully");
          }
        }
      }

      @Override
      public void onNewJobModel(String processorId, JobModel jobModel) {
        if (!jobModel.getContainers().containsKey(processorId)) {
          LOGGER.warn("JobModel did not contain the processorId. Stopping the processor.");
          stop();
        } else {
          jcContainerLatch = new CountDownLatch(1);

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
              }
            }

            @Override
            public void onContainerStop(boolean pauseByJm) {
              if (jcContainerLatch != null) {
                jcContainerLatch.countDown();
              } else {
                LOGGER.warn("JobCoordinatorLatch was null. It is possible for some component to be waiting.");
              }

              if (pauseByJm) {
                LOGGER.info("Container " + container.toString() + " stopped due to a request from JobCoordinator.");
              } else {  // sp.stop was called or container stopped by itself
                LOGGER.info("Container " + container.toString() + " stopped.");
                container = null; // this guarantees that stop() doesn't try to stop container again
                stop();
              }
            }

            @Override
            public void onContainerFailed(Throwable t) {
              if (jcContainerLatch != null) {
                jcContainerLatch.countDown();
              } else {
                LOGGER.warn("JobCoordinatorLatch was null. It is possible for some component to be waiting.");
              }
              LOGGER.info("Container failed with " + t + ". Stopping the processor.");
              stop();
            }
          };

          container = createSamzaContainer(
              jobModel.getContainers().get(processorId),
              jobModel.maxChangeLogStreamPartitions,
              new JmxServer());
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
   * @param processorId            String identifier for this processor
   * @param config                 Instance of config object - contains all configuration required for processing
   * @param customMetricsReporters Map of custom MetricReporter instances that are to be injected in the Samza job
   * @param asyncStreamTaskFactory The {@link AsyncStreamTaskFactory} to be used for creating task instances.
   * @param processorListener         listener to the StreamProcessor life cycle
   */
  public StreamProcessor(String processorId, Config config, Map<String, MetricsReporter> customMetricsReporters,
                         AsyncStreamTaskFactory asyncStreamTaskFactory, StreamProcessorLifecycleListener processorListener) {
    this(processorId, config, customMetricsReporters, (Object) asyncStreamTaskFactory, processorListener);
  }

  /**
   *Same as {@link #StreamProcessor(String, Config, Map, AsyncStreamTaskFactory, StreamProcessorLifecycleListener)}, except task
   * instances are created using the provided {@link StreamTaskFactory}.
   * @param config - config
   * @param customMetricsReporters metric Reporter
   * @param streamTaskFactory task factory to instantiate the Task
   * @param processorListener  listener to the StreamProcessor life cycle
   */
  public StreamProcessor(String processorId, Config config, Map<String, MetricsReporter> customMetricsReporters,
                         StreamTaskFactory streamTaskFactory, StreamProcessorLifecycleListener processorListener) {
    this(processorId, config, customMetricsReporters, (Object) streamTaskFactory, processorListener);
  }

  /* package private */
  JobCoordinator getJobCoordinator(String processorId) {
    return Util.
        <JobCoordinatorFactory>getObj(
            new JobCoordinatorConfig(config)
                .getJobCoordinatorFactoryClassName())
        .getJobCoordinator(processorId, config);
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

  private StreamProcessor(String processorId, Config config, Map<String, MetricsReporter> customMetricsReporters,
                          Object taskFactory, StreamProcessorLifecycleListener processorListener) {
    this.taskFactory = taskFactory;
    this.config = config;
    this.taskShutdownMs = new TaskConfigJava(config).getShutdownMs();
    this.customMetricsReporter = customMetricsReporters;
    this.processorListener = processorListener;
    this.jobCoordinator = getJobCoordinator(processorId);
    this.jobCoordinator.setListener(createJobCoordinatorListener());
  }

  /**
   * Starts the JobCoordinator in StreamProcessor.
   */
  public void start() {
    jobCoordinator.start();
  }

  /**
   * Stops the Streamprocessor's running components.
   *
   * Can be invoked by the user of StreamProcessor api or by any of the components in StreamProcessor (JobCoordinator
   * or SamzaContainer)
   */
  public synchronized void stop() {
    if (container != null) {
      LOGGER.info("Shutting down container " + container.toString() + " from StreamProcessor");
      container.shutdown(false);
    } else {
      LOGGER.info("Shutting down JobCoordinator from StreamProcessor");
      jobCoordinator.stop();
    }
  }
}
