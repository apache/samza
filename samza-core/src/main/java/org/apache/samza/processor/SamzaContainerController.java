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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SamzaContainerController {
  private static final Logger log = LoggerFactory.getLogger(SamzaContainerController.class);

  private volatile SamzaContainer container;
  private final Map<String, MetricsReporter> metricsReporterMap;
  private final Object taskFactory;
  private final ProcessorLifecycleCallback lifecycleCallback;

  // Internal Member Variables
  private Future containerFuture;
  private ExecutorService executorService;

  /**
   * Creates an instance of a controller for instantiating, starting and/or stopping {@link SamzaContainer}
   * Requests to execute a container are submitted to the {@link ExecutorService}
   *
   * @param taskFactory         Factory that be used create instances of {@link org.apache.samza.task.StreamTask} or
   *                            {@link org.apache.samza.task.AsyncStreamTask}
   * @param lifecycleCallback   An instance of callback handler for lifecycle events. May be null. If null, the caller
   *                            will not get notified on the lifecycle events.
   * @param metricsReporterMap  Map of metric reporter name and {@link MetricsReporter} instance
   */
  public SamzaContainerController(
      Object taskFactory,
      ProcessorLifecycleCallback lifecycleCallback,
      Map<String, MetricsReporter> metricsReporterMap) {
    this.taskFactory = taskFactory;
    this.metricsReporterMap = metricsReporterMap;
    this.lifecycleCallback = lifecycleCallback;
  }

  /**
   * Instantiates a container and submits to the executor. This method does not actually wait for the container to
   * fully start-up. For such a behavior, see {@link #awaitStart(long)}
   * <p>
   * <b>Note:</b> <i>This method does not stop a currently running container, if any. It is left up to the caller to
   * ensure that the container has been stopped with stopContainer before invoking this method.</i>
   *
   * @param containerModel               {@link ContainerModel} instance to use for the current run of the Container
   * @param config                       Complete configuration map used by the Samza job
   * @param maxChangelogStreamPartitions Max number of partitions expected in the changelog streams
   *                                     TODO: Try to get rid of maxChangelogStreamPartitions from method arguments
   */
  public void startContainer(ContainerModel containerModel, Config config, int maxChangelogStreamPartitions) {
    LocalityManager localityManager = null;
    if (new ClusterManagerConfig(config).getHostAffinityEnabled()) {
      localityManager = SamzaContainerWrapper.getLocalityManager(containerModel.getContainerId(), config);
    }
    log.info("About to create container: " + containerModel.getContainerId());

    container = SamzaContainerWrapper.createInstance(
        containerModel.getContainerId(),
        containerModel,
        config,
        maxChangelogStreamPartitions,
        localityManager,
        new JmxServer(),
        Util.<String, MetricsReporter>javaMapAsScalaMap(metricsReporterMap),
        taskFactory,
        // TODO: need to use the correct local ApplicationRunner here
        null);
    log.info("About to start container: " + containerModel.getContainerId());
    executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("processor-thread-%d").build());

    containerFuture = executorService.submit(() -> {
        try {
          container.run();
        } catch (Throwable t) {
          if (lifecycleCallback != null) {
            lifecycleCallback.onError(t);
          }
        }
      });
  }

  boolean isContainerRunning() {
    return container != null && containerFuture != null && !containerFuture.isDone();
  }

  /**
   * Method waits for a specified amount of time for the container to fully start-up, which consists of class-loading
   * all the components and start message processing
   *
   * @param timeoutMs Maximum time to wait, in milliseconds
   * @return {@code true}, if the container started within the specified wait time and {@code false} if the waiting
   * time elapsed
   * @throws InterruptedException if the current thread is interrupted while waiting for container to start-up
   */
  public boolean awaitStart(long timeoutMs) throws InterruptedException {
    if (container == null) {
      log.warn("Cannot awaitStart when container has not been started!");
      return false;
    }
    return container.awaitStart(timeoutMs);
  }

  public boolean awaitStop(long timeoutMs) throws InterruptedException {
    if (container == null) {
      log.warn("Cannot awaitStop when container has not been started!" );
      return false;
    }
    return container.awaitStop(timeoutMs);
  }

  /**
   * Stops a running container, if any. Invoking this method multiple times does not have any side-effects.
   */
  public void stopContainer() {
    if (container == null) {
      log.warn("stopContainer called before a container was created.");
      return;
    }
    container.shutdown();
    executorService.shutdown();
  }
}
