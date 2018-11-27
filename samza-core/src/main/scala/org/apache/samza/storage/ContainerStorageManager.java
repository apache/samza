package org.apache.samza.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstance;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.system.SystemConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContainerStorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerStorageManager.class);
  private Map<TaskInstance, TaskStorageManager> taskStorageManagers;
  private SamzaContainerMetrics samzaContainerMetrics;

  // Mapping of from storeSystemNames to SystemConsumers
  private Map<String, SystemConsumer> systemConsumers;

  // Size of thread-pool to be used for parallel restores
  private int parallelRestoreThreadPoolSize;

  public ContainerStorageManager(Map<TaskInstance, TaskStorageManager> taskStorageManagers,
      Map<String, SystemConsumer> systemConsumers, SamzaContainerMetrics samzaContainerMetrics) {
    this.taskStorageManagers = taskStorageManagers;
    this.systemConsumers = systemConsumers;
    this.samzaContainerMetrics = samzaContainerMetrics;

    // Setting thread pool size equal to the number of tasks
    this.parallelRestoreThreadPoolSize = taskStorageManagers.size();
  }

  public void start() throws InterruptedException {

    // initialize each TaskStorageManager
    this.taskStorageManagers.values().forEach(taskStorageManager -> taskStorageManager.init());

    // Start consumers
    this.systemConsumers.values().forEach(systemConsumer -> systemConsumer.start());

    // Create a thread pool for parallel restores
    ExecutorService executorService = Executors.newFixedThreadPool(this.parallelRestoreThreadPoolSize,
        new ThreadFactoryBuilder().setNameFormat("Samza Restore Thread-%d").build());

    this.taskStorageManagers.entrySet().forEach(task -> {
      executorService.submit(new TaskRestoreRunnable(this.samzaContainerMetrics, task.getKey(), task.getValue()));
    });

    executorService.shutdown();

    // Wait forever for all threads to finish (java-suggested pattern)
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);

    // Stop consumers
    this.systemConsumers.values().forEach(systemConsumer -> systemConsumer.stop());
  }

  public void shutdown() {
    this.taskStorageManagers.forEach((taskInstance, taskStorageManager) -> {
      if (taskStorageManager != null) {
        LOG.debug("Shutting down storage manager for taskName: {} ", taskInstance);
        taskStorageManager.stopStores();
      } else {
        LOG.debug("Skipping storage manager shutdown for taskName: {}", taskInstance);
      }
    });
  }

  private class TaskRestoreRunnable implements Runnable {

    private TaskInstance taskInstance;
    private TaskStorageManager taskStorageManager;
    private SamzaContainerMetrics samzaContainerMetrics;

    public TaskRestoreRunnable(SamzaContainerMetrics samzaContainerMetrics, TaskInstance taskInstance,
        TaskStorageManager taskStorageManager) {
      this.samzaContainerMetrics = samzaContainerMetrics;
      this.taskInstance = taskInstance;
      this.taskStorageManager = taskStorageManager;
    }

    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      LOG.info("Starting stores in task instance {}", this.taskInstance.taskName().getTaskName());
      taskStorageManager.restoreStores();
      long timeToRestore = System.currentTimeMillis() - startTime;
      Gauge taskGauge = this.samzaContainerMetrics.taskStoreRestorationMetrics()
          .getOrDefault(this.taskInstance.taskName().getTaskName(), null);

      if (taskGauge != null) {
        taskGauge.set(timeToRestore);
      }
    }
  }
}
