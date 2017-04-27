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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.util.Util;

import java.util.Map;

/**
 * StreamProcessor can be embedded in any application or executed in a distributed environment (aka cluster) as an
 * independent process.
 * <p>
 * <b>Usage Example:</b>
 * <pre>
 * StreamProcessor processor = new StreamProcessor(1, config);
 * processor.start();
 * try {
 *  boolean status = processor.awaitStart(TIMEOUT_MS);    // Optional - blocking call
 *  if (!status) {
 *    // Timed out
 *  }
 *  ...
 * } catch (InterruptedException ie) {
 *   ...
 * } finally {
 *   processor.stop();
 * }
 * </pre>
 * Note: A single JVM can create multiple StreamProcessor instances. It is safe to create StreamProcessor instances in
 * multiple threads.
 */
@InterfaceStability.Evolving
public class StreamProcessor {
  private final JobCoordinator jobCoordinator;
  private final StreamProcessorLifecycleListener lifecycleListener;

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
   * @param lifecycleListener         listener to the StreamProcessor life cycle
   */
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters,
                         AsyncStreamTaskFactory asyncStreamTaskFactory, StreamProcessorLifecycleListener lifecycleListener) {
    this(config, customMetricsReporters, (Object) asyncStreamTaskFactory, lifecycleListener);
  }


  /**
   * Same as {@link #StreamProcessor(Config, Map, AsyncStreamTaskFactory, StreamProcessorLifecycleListener)}, except task
   * instances are created using the provided {@link StreamTaskFactory}.
   * @param config - config
   * @param customMetricsReporters metric Reporter
   * @param streamTaskFactory task factory to instantiate the Task
   * @param lifecycleListener  listener to the StreamProcessor life cycle
   */
  public StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters,
                         StreamTaskFactory streamTaskFactory, StreamProcessorLifecycleListener lifecycleListener) {
    this(config, customMetricsReporters, (Object) streamTaskFactory, lifecycleListener);
  }

  private StreamProcessor(Config config, Map<String, MetricsReporter> customMetricsReporters,
                          Object taskFactory, StreamProcessorLifecycleListener lifecycleListener) {
    SamzaContainerController containerController = new SamzaContainerController(
        taskFactory,
        new TaskConfigJava(config).getShutdownMs(),
        customMetricsReporters,
        lifecycleListener);

    this.jobCoordinator = Util.
        <JobCoordinatorFactory>getObj(
            new JobCoordinatorConfig(config)
                .getJobCoordinatorFactoryClassName())
        .getJobCoordinator(config, containerController);

    this.lifecycleListener = lifecycleListener;
  }

  /**
   * StreamProcessor Lifecycle: start()
   * <ul>
   * <li>Starts the JobCoordinator and fetches the JobModel</li>
   * <li>jobCoordinator.start returns after starting the container using ContainerModel </li>
   * </ul>
   * When start() returns, it only guarantees that the container is initialized and submitted by the controller to
   * execute
   */
  public void start() {
    jobCoordinator.start();
    lifecycleListener.onStart();
  }

  /**
   * Method that allows the user to wait for a specified amount of time for the container to initialize and start
   * processing messages
   *
   * @param timeoutMs Maximum time to wait, in milliseconds
   * @return {@code true}, if the container started within the specified wait time and {@code false} if the waiting time
   * elapsed
   * @throws InterruptedException if the current thread is interrupted while waiting for container to start-up
   */
  public boolean awaitStart(long timeoutMs) throws InterruptedException {
    return jobCoordinator.awaitStart(timeoutMs);
  }

  /**
   * StreamProcessor Lifecycle: stop()
   * <ul>
   * <li>Stops the SamzaContainer execution</li>
   * <li>Stops the JobCoordinator</li>
   * </ul>
   */
  public void stop() {
    jobCoordinator.stop();
  }
}
