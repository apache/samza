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
package org.apache.samza.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.system.SystemConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  ContainerStorageManager is a per-container object that manages
 *  the restore of per-task partitions.
 *
 *  It is responsible for
 *  a) performing all container-level actions for restore such as, initializing and shutting down
 *  taskStorage managers, starting, registering and stopping consumers, etc.
 *
 *  b) performing individual taskStorageManager restores in parallel.
 *
 */
public class ContainerStorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerStorageManager.class);
  private final Map<TaskName, TaskStorageManager> taskStorageManagers;
  private final SamzaContainerMetrics samzaContainerMetrics;

  // Mapping of from storeSystemNames to SystemConsumers
  private final Map<String, SystemConsumer> systemConsumers;

  // Size of thread-pool to be used for parallel restores
  private final int parallelRestoreThreadPoolSize;

  // Naming convention to be used for restore threads
  private static final String RESTORE_THREAD_NAME = "Samza Restore Thread-%d";

  public ContainerStorageManager(Map<TaskName, TaskStorageManager> taskStorageManagers,
      Map<String, SystemConsumer> systemConsumers, SamzaContainerMetrics samzaContainerMetrics) {
    this.taskStorageManagers = taskStorageManagers;
    this.systemConsumers = systemConsumers;
    this.samzaContainerMetrics = samzaContainerMetrics;

    // Setting thread pool size equal to the number of tasks
    this.parallelRestoreThreadPoolSize = taskStorageManagers.size();
  }

  public void start() throws SamzaException {
    LOG.info("Restore started with tsms: {} sysConsumers: {}", this.taskStorageManagers, this.systemConsumers);

    // initialize each TaskStorageManager
    this.taskStorageManagers.values().forEach(taskStorageManager -> taskStorageManager.init());

    // Start consumers
    this.systemConsumers.values().forEach(systemConsumer -> systemConsumer.start());

    // Create a thread pool for parallel restores
    ExecutorService executorService = Executors.newFixedThreadPool(this.parallelRestoreThreadPoolSize,
        new ThreadFactoryBuilder().setNameFormat(RESTORE_THREAD_NAME).build());

    List<Future> taskRestoreFutures = new ArrayList<>(this.taskStorageManagers.entrySet().size());

    // Submit restore callable for each taskInstance
    this.taskStorageManagers.forEach((taskInstance, taskStorageManager) -> {
        taskRestoreFutures.add(
            executorService.submit(new TaskRestoreCallable(this.samzaContainerMetrics, taskInstance, taskStorageManager)));
      });

    // loop-over the future list to wait for each thread to finish, catch any exceptions during restore and throw
    // as samza exceptions
    for (Future future : taskRestoreFutures) {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("Exception when restoring ", e);
        throw new SamzaException("Exception when restoring ", e);
      }
    }

    executorService.shutdown();

    // Stop consumers
    this.systemConsumers.values().forEach(systemConsumer -> systemConsumer.stop());

    LOG.info("Restore complete");
  }

  public void shutdown() {
    this.taskStorageManagers.forEach((taskInstance, taskStorageManager) -> {
        if (taskStorageManager != null) {
          LOG.debug("Shutting down task storage manager for taskName: {} ", taskInstance);
          taskStorageManager.stop();
        } else {
          LOG.debug("Skipping task storage manager shutdown for taskName: {}", taskInstance);
        }
      });

    LOG.info("Shutdown complete");
  }

  /** Callable for performing the restoreStores on a taskStorage manager and emitting task-restoration metric.
   *
   */
  private class TaskRestoreCallable implements Callable<Void> {

    private TaskName taskName;
    private TaskStorageManager taskStorageManager;
    private SamzaContainerMetrics samzaContainerMetrics;

    public TaskRestoreCallable(SamzaContainerMetrics samzaContainerMetrics, TaskName taskName,
        TaskStorageManager taskStorageManager) {
      this.samzaContainerMetrics = samzaContainerMetrics;
      this.taskName = taskName;
      this.taskStorageManager = taskStorageManager;
    }

    @Override
    public Void call() {
      long startTime = System.currentTimeMillis();
      LOG.info("Starting stores in task instance {}", this.taskName.getTaskName());
      taskStorageManager.restoreStores();
      long timeToRestore = System.currentTimeMillis() - startTime;

      if (this.samzaContainerMetrics != null) {
        Gauge taskGauge = this.samzaContainerMetrics.taskStoreRestorationMetrics().getOrDefault(this.taskName, null);

        if (taskGauge != null) {
          taskGauge.set(timeToRestore);
        }
      }
      return null;
    }
  }
}
