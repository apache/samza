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
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionIterator;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


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

  private final Map<TaskName, TaskRestoreManager> taskRestoreManagers;

  private final SamzaContainerMetrics samzaContainerMetrics;

  private final Map<TaskName, Map<String, StorageEngine>> taskStores;

  // Mapping of from storeSystemNames to SystemConsumers
  private final Map<String, SystemConsumer> systemConsumers;

  // Size of thread-pool to be used for parallel restores
  private final int parallelRestoreThreadPoolSize;

  // Naming convention to be used for restore threads
  private static final String RESTORE_THREAD_NAME = "Samza Restore Thread-%d";

  private final Config config;
  private final int maxChangeLogStreamPartitions;
  private final StreamMetadataCache streamMetadataCache;

  // the set of store directory paths, used by SamzaContainer to initialize its disk-space-monitor
  private final Set<Path> storeDirectoryPaths;

  public ContainerStorageManager(ContainerModel containerModel, StreamMetadataCache streamMetadataCache,
      SystemAdmins systemAdmins, Map<String, SystemStream> changelogSystemStreams,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemFactory> systemFactories, Map<String, Serde<Object>> serdes, Config config,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics, SamzaContainerMetrics samzaContainerMetrics,
      JobContext jobContext, ContainerContext containerContext,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors, int maxChangeLogStreamPartitions) {

    // set the config
    this.config = config;

    // initialize the taskStores
    this.taskStores =
        createTaskStores(containerModel, jobContext, containerContext, storageEngineFactories, changelogSystemStreams,
            serdes, taskInstanceMetrics, taskInstanceCollectors);

    // initializing the set of store directory paths
    this.storeDirectoryPaths = new HashSet<>();

    // create system consumers (1 per store system)
    this.systemConsumers = createStoreConsumers(changelogSystemStreams, systemFactories, config);

    // Setting the metrics registry
    this.samzaContainerMetrics = samzaContainerMetrics;

    // Setting the restore thread pool size equal to the number of taskInstances
    this.parallelRestoreThreadPoolSize = taskInstanceMetrics.size();

    this.maxChangeLogStreamPartitions = maxChangeLogStreamPartitions;
    this.streamMetadataCache = streamMetadataCache;

    // creating task restore managers
    this.taskRestoreManagers = containerModel.getTasks().entrySet().stream().
        collect(Collectors.toMap(entry -> entry.getKey(),
            entry -> new TaskRestoreManager(entry.getValue(), changelogSystemStreams, taskStores.get(entry.getKey()),
                systemAdmins)));
  }

  private Map<String, SystemConsumer> createStoreConsumers(Map<String, SystemStream> changelogSystemStreams,
      Map<String, SystemFactory> systemFactories, Config config) {
    // Determine the set of systems being used across all stores
    Set<String> storeSystems =
        changelogSystemStreams.values().stream().map(SystemStream::getSystem).collect(Collectors.toSet());

    // Create one consumer for each system in use
    Map<String, SystemConsumer> storeSystemConsumers = new HashMap<>();

    for (String storeSystemName : storeSystems) {
      SystemFactory systemFactory = systemFactories.get(storeSystemName);
      if (systemFactory == null) {
        throw new SamzaException("Changelog system " + storeSystemName + " does not exist in config");
      }
      storeSystemConsumers.put(storeSystemName,
          systemFactory.getConsumer(storeSystemName, config, samzaContainerMetrics.registry()));
    }

    // Populate the map of storeName to its relevant systemConsumer
    return changelogSystemStreams.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, x -> storeSystemConsumers.get(x.getValue().getSystem())));
  }

  private Map<TaskName, Map<String, StorageEngine>> createTaskStores(ContainerModel containerModel,
      JobContext jobContext, ContainerContext containerContext,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemStream> changelogSystemStreams, Map<String, Serde<Object>> serdes,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors) {

    Map<TaskName, Map<String, StorageEngine>> taskStoresMap = new HashMap<>();

    for (Map.Entry<TaskName, TaskModel> task : containerModel.getTasks().entrySet()) {
      TaskName taskName = task.getKey();
      TaskModel taskModel = task.getValue();
      StorageConfig storageConfig = new StorageConfig(config);

      for (String storeName : storageEngineFactories.keySet()) {

        SystemStreamPartition changeLogSystemStreamPartition =
            (changelogSystemStreams.containsKey(storeName)) ? new SystemStreamPartition(
                changelogSystemStreams.get(storeName), taskModel.getChangelogPartition()) : null;

        Serde keySerde = serdes.get(storageConfig.getStorageKeySerde(storeName));
        if (keySerde == null) {
          throw new SamzaException(
              "StorageKeySerde: No class defined for serde: " + storageConfig.getStorageKeySerde(storeName));
        }

        Serde messageSerde = serdes.get(storageConfig.getStorageMsgSerde(storeName));
        if (messageSerde == null) {
          throw new SamzaException(
              "StorageMsgSerde: No class defined for serde: " + storageConfig.getStorageMsgSerde(storeName));
        }

        // We use the logged storage base directory for change logged stores
        File storeDirectory = (changeLogSystemStreamPartition != null) ? ContainerStorageManager.getStorePartitionDir(
            getLoggedStorageBaseDir(), storeName, taskName)
            : ContainerStorageManager.getStorePartitionDir(getNonLoggedStorageBaseDir(), storeName, taskName);

        StorageEngine storageEngine = storageEngineFactories.get(storeName)
            .getStorageEngine(storeName, storeDirectory, keySerde, messageSerde, taskInstanceCollectors.get(taskName),
                taskInstanceMetrics.get(taskName).registry(), changeLogSystemStreamPartition, jobContext,
                containerContext);

        this.storeDirectoryPaths.add(storeDirectory.toPath());

        if (taskStoresMap.get(taskName) == null) {
          taskStoresMap.put(taskName, new HashMap<>());
        }
        taskStoresMap.get(taskName).put(storeName, storageEngine);
      }
    }
    return taskStoresMap;
  }

  public void start() throws SamzaException {
    LOG.info("Restore started");

    // initialize each TaskStorageManager
    this.taskRestoreManagers.values().forEach(taskStorageManager -> taskStorageManager.initialize());

    // Start consumers
    this.systemConsumers.values().forEach(systemConsumer -> systemConsumer.start());

    // Create a thread pool for parallel restores
    ExecutorService executorService = Executors.newFixedThreadPool(this.parallelRestoreThreadPoolSize,
        new ThreadFactoryBuilder().setNameFormat(RESTORE_THREAD_NAME).build());

    List<Future> taskRestoreFutures = new ArrayList<>(this.taskRestoreManagers.entrySet().size());

    // Submit restore callable for each taskInstance
    this.taskRestoreManagers.forEach((taskInstance, taskRestoreManager) -> {
        taskRestoreFutures.add(executorService.submit(
            new TaskRestoreCallable(this.samzaContainerMetrics, taskInstance, taskRestoreManager)));
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

  public Optional<StorageEngine> getStore(TaskName taskName, String storeName) {
    return Optional.ofNullable(this.taskStores.get(taskName).get(storeName));
  }

  public Map<String, StorageEngine> getAllStores(TaskName taskName) {
    return this.taskStores.get(taskName);
  }

  public Set<Path> getStoreDirectoryPaths() {
    return this.storeDirectoryPaths;
  }

  private final File getDefaultStoreBaseDir() {
    File defaultStoreBaseDir = new File(System.getProperty("user.dir"), "state");
    LOG.info("Got default storage engine base directory " + defaultStoreBaseDir);
    return defaultStoreBaseDir;
  }

  public final File getNonLoggedStorageBaseDir() {
    JobConfig jobConfig = new JobConfig(this.config);

    if (jobConfig.getNonLoggedStorePath().isDefined()) {
      return new File(jobConfig.getNonLoggedStorePath().get());
    } else {
      return getDefaultStoreBaseDir();
    }
  }

  public final File getLoggedStorageBaseDir() {
    JobConfig jobConfig = new JobConfig(this.config);

    File defaultLoggedStorageBaseDir, loggedStorageBaseDir;

    if (jobConfig.getLoggedStorePath().isDefined()) {
      defaultLoggedStorageBaseDir = new File(jobConfig.getLoggedStorePath().get());
    } else {
      defaultLoggedStorageBaseDir = getDefaultStoreBaseDir();
    }

    if (System.getenv(ShellCommandConfig.ENV_LOGGED_STORE_BASE_DIR()) != null) {

      if (!jobConfig.getName().isDefined()) {
        throw new ConfigException("Missing required config: job.name");
      }

      loggedStorageBaseDir = new File(
          System.getenv(ShellCommandConfig.ENV_LOGGED_STORE_BASE_DIR()) + File.separator + jobConfig.getName().get()
              + "-" + jobConfig.getJobId());
    } else {
      if (jobConfig.getLoggedStorePath().isEmpty()) {
        LOG.warn("No override was provided for logged store base directory. This disables local state re-use on "
            + "application restart. If you want to enable this feature, set LOGGED_STORE_BASE_DIR as an environment "
            + "variable in all machines running the Samza container or configure job.logged.store.base.dir for your application");
      }

      loggedStorageBaseDir = defaultLoggedStorageBaseDir;
    }

    return loggedStorageBaseDir;
  }

  public static File getStorePartitionDir(File storeBaseDir, String storeName, TaskName taskName) {
    // TODO: Sanitize, check and clean taskName string as a valid value for a file
    return new File(storeBaseDir, (storeName + File.separator + taskName.toString()).replace(' ', '_'));
  }

  public void shutdown() {
    this.taskRestoreManagers.forEach((taskInstance, taskRestoreManager) -> {
        if (taskRestoreManager != null) {
          LOG.debug("Shutting down task storage manager for taskName: {} ", taskInstance);
          taskRestoreManager.stop();
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
    private TaskRestoreManager taskRestoreManager;
    private SamzaContainerMetrics samzaContainerMetrics;

    public TaskRestoreCallable(SamzaContainerMetrics samzaContainerMetrics, TaskName taskName,
        TaskRestoreManager taskRestoreManager) {
      this.samzaContainerMetrics = samzaContainerMetrics;
      this.taskName = taskName;
      this.taskRestoreManager = taskRestoreManager;
    }

    @Override
    public Void call() {
      long startTime = System.currentTimeMillis();
      LOG.info("Starting stores in task instance {}", this.taskName.getTaskName());
      taskRestoreManager.restoreStores();
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

  /**
   * Restore logic for a task including directory cleanup, setup, changelogSSP validation, and registering with the consumer
   */
  private class TaskRestoreManager {

    private Map<String, StorageEngine> taskStores;
    private TaskModel taskModel;
    private long currentTimeMillis;
    private final static String OFFSET_FILE_NAME = "OFFSET";
    private Map<SystemStream, String> changeLogOldestOffsets;
    private Map<SystemStreamPartition, String> fileOffsets;
    private final Map<String, SystemStream> changelogSystemStreams;
    private final SystemAdmins systemAdmins;

    public TaskRestoreManager(TaskModel taskModel, Map<String, SystemStream> changelogSystemStreams,
        Map<String, StorageEngine> taskStores, SystemAdmins systemAdmins) {
      this.taskStores = taskStores;
      this.taskModel = taskModel;
      this.currentTimeMillis = System.currentTimeMillis();
      this.changelogSystemStreams = changelogSystemStreams;
      this.systemAdmins = systemAdmins;
      this.fileOffsets = new HashMap<>();
    }

    public void initialize() {
      cleanBaseDirs();
      setupBaseDirs();
      validateChangelogStreams();
      registerSSPs();
    }

    private void cleanBaseDirs() {
      LOG.debug("Cleaning base directories for stores.");

      taskStores.keySet().forEach(storeName -> {
          File nonLoggedStorePartitionDir =
              ContainerStorageManager.getStorePartitionDir(getNonLoggedStorageBaseDir(), storeName,
                  taskModel.getTaskName());
          LOG.info("Got non logged storage partition directory as " + nonLoggedStorePartitionDir.toPath().toString());

          if (nonLoggedStorePartitionDir.exists()) {
            LOG.info("Deleting non logged storage partition directory " + nonLoggedStorePartitionDir.toPath().toString());
            FileUtil.rm(nonLoggedStorePartitionDir);
          }

          File loggedStorePartitionDir =
              ContainerStorageManager.getStorePartitionDir(getLoggedStorageBaseDir(), storeName, taskModel.getTaskName());
          LOG.info("Got logged storage partition directory as " + loggedStorePartitionDir.toPath().toString());

          // Delete the logged store if it is not valid.
          if (!isLoggedStoreValid(storeName, loggedStorePartitionDir)) {
            LOG.info("Deleting logged storage partition directory " + loggedStorePartitionDir.toPath().toString());
            FileUtil.rm(loggedStorePartitionDir);
          } else {
            String offset = StorageManagerUtil.readOffsetFile(loggedStorePartitionDir, OFFSET_FILE_NAME);
            LOG.info("Read offset " + offset + " for the store " + storeName + " from logged storage partition directory "
                + loggedStorePartitionDir);

            if (offset != null) {
              fileOffsets.put(
                  new SystemStreamPartition(changelogSystemStreams.get(storeName), taskModel.getChangelogPartition()),
                  offset);
            }
          }
        });
    }

    /**
     * Directory loggedStoreDir associated with the logged store storeName is valid
     * if all of the following conditions are true.
     * a) If the store has to be persisted to disk.
     * b) If there is a valid offset file associated with the logged store.
     * c) If the logged store has not gone stale.
     *
     * @return true if the logged store is valid, false otherwise.
     */
    private boolean isLoggedStoreValid(String storeName, File loggedStoreDir) {
      long changeLogDeleteRetentionInMs = StorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS();

      if (new StorageConfig(config).getChangeLogDeleteRetentionsInMs().get(storeName).isDefined()) {
        changeLogDeleteRetentionInMs =
            (long) new StorageConfig(config).getChangeLogDeleteRetentionsInMs().get(storeName).get();
      }

      return this.taskStores.get(storeName).getStoreProperties().isPersistedToDisk()
          && StorageManagerUtil.isOffsetFileValid(loggedStoreDir, OFFSET_FILE_NAME) && !StorageManagerUtil.isStaleStore(
          loggedStoreDir, OFFSET_FILE_NAME, changeLogDeleteRetentionInMs, currentTimeMillis);
    }

    private void setupBaseDirs() {
      LOG.debug("Setting up base directories for stores.");
      taskStores.forEach((storeName, storageEngine) -> {
          if (storageEngine.getStoreProperties().isLoggedStore()) {

            File loggedStorePartitionDir =
                ContainerStorageManager.getStorePartitionDir(getLoggedStorageBaseDir(), storeName,
                    taskModel.getTaskName());

            LOG.info("Using logged storage partition directory: " + loggedStorePartitionDir.toPath().toString()
                + " for store: " + storeName);

            if (!loggedStorePartitionDir.exists()) {
              loggedStorePartitionDir.mkdirs();
            }
          } else {
            File nonLoggedStorePartitionDir =
                ContainerStorageManager.getStorePartitionDir(getNonLoggedStorageBaseDir(), storeName,
                    taskModel.getTaskName());
            LOG.info("Using non logged storage partition directory: " + nonLoggedStorePartitionDir.toPath().toString()
                + " for store: " + storeName);
          }
        });
    }

    private void validateChangelogStreams() {
      LOG.info("Validating change log streams: " + changelogSystemStreams);

      for (SystemStream changelogSystemStream : changelogSystemStreams.values()) {
        SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(changelogSystemStream.getSystem());
        StreamSpec changelogSpec =
            StreamSpec.createChangeLogStreamSpec(changelogSystemStream.getStream(), changelogSystemStream.getSystem(),
                maxChangeLogStreamPartitions);

        systemAdmin.validateStream(changelogSpec);
      }

      Map<SystemStream, SystemStreamMetadata> changeLogMetadata = JavaConverters.mapAsJavaMapConverter(
          streamMetadataCache.getStreamMetadata(
              JavaConverters.asScalaSetConverter(new HashSet<>(changelogSystemStreams.values())).asScala().toSet(),
              false)).asJava();
      LOG.info("Got change log stream metadata: " + changeLogMetadata);

      changeLogOldestOffsets =
          getChangeLogOldestOffsetsForPartition(taskModel.getChangelogPartition(), changeLogMetadata);
      LOG.info(
          "Assigning oldest change log offsets for taskName " + taskModel.getTaskName() + ":" + changeLogOldestOffsets);
    }

    /**
     * Builds a map from SystemStreamPartition to oldest offset for changelogs.
     */
    private Map<SystemStream, String> getChangeLogOldestOffsetsForPartition(Partition partition,
        Map<SystemStream, SystemStreamMetadata> inputStreamMetadata) {

      return inputStreamMetadata.entrySet()
          .stream()
          .filter(x -> x.getValue().getSystemStreamPartitionMetadata().get(partition) != null)
          .collect(Collectors.toMap(e -> e.getKey(),
              e -> e.getValue().getSystemStreamPartitionMetadata().get(partition).getOldestOffset()));
    }

    private void registerSSPs() {
      LOG.debug("Starting consumers for stores.");

      for (Map.Entry<String, SystemStream> changelogSystemStreamEntry : changelogSystemStreams.entrySet()) {
        SystemStreamPartition systemStreamPartition =
            new SystemStreamPartition(changelogSystemStreamEntry.getValue(), taskModel.getChangelogPartition());
        SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(changelogSystemStreamEntry.getValue().getSystem());
        SystemConsumer systemConsumer = systemConsumers.get(changelogSystemStreamEntry.getKey());

        String offset = getStartingOffset(systemStreamPartition, systemAdmin);

        if (offset != null) {
          LOG.info("Registering change log consumer with offset " + offset + " for %" + systemStreamPartition);
          systemConsumer.register(systemStreamPartition, offset);
        } else {
          LOG.info("Skipping change log restoration for " + systemStreamPartition
              + " because stream appears to be empty (offset was null).");
        }
      }
    }

    /**
     * Returns the offset with which the changelog consumer should be initialized for the given SystemStreamPartition.
     *
     * If a file offset exists, it represents the last changelog offset which is also reflected in the on-disk state.
     * In that case, we use the next offset after the file offset, as long as it is newer than the oldest offset
     * currently available in the stream.
     *
     * If there isn't a file offset or it's older than the oldest available offset, we simply start with the oldest.
     *
     * @param systemStreamPartition  the changelog partition for which the offset is needed.
     * @param systemAdmin                  the [[SystemAdmin]] for the changelog.
     * @return the offset to from which the changelog consumer should be initialized.
     */
    private String getStartingOffset(SystemStreamPartition systemStreamPartition, SystemAdmin systemAdmin) {
      String fileOffset = fileOffsets.get(systemStreamPartition);
      String oldestOffset = changeLogOldestOffsets.get(systemStreamPartition.getSystemStream());

      if (oldestOffset == null) {
        throw new SamzaException("Missing a change log offset for " + systemStreamPartition);
      }

      return StorageManagerUtil.getStartingOffset(systemStreamPartition, systemAdmin, fileOffset, oldestOffset);
    }

    public void restoreStores() {
      LOG.debug("Restoring stores for task: " + taskModel.getTaskName());

      for (Map.Entry<String, StorageEngine> store : taskStores.entrySet()) {
        if (store.getValue().getStoreProperties().isPersistedToDisk()) {
          SystemConsumer systemConsumer = systemConsumers.get(store.getKey());
          SystemStream systemStream = changelogSystemStreams.get(store.getKey());

          SystemStreamPartitionIterator systemStreamPartitionIterator =
              new SystemStreamPartitionIterator(systemConsumer,
                  new SystemStreamPartition(systemStream, taskModel.getChangelogPartition()));

          store.getValue().restore(systemStreamPartitionIterator);
        }
      }
    }

    public void stop() {

    }
  }
}
