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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.BlobStoreConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.storage.blobstore.BlobStoreBackupManager;
import org.apache.samza.storage.blobstore.BlobStoreManager;
import org.apache.samza.storage.blobstore.BlobStoreManagerFactory;
import org.apache.samza.storage.blobstore.BlobStoreRestoreManager;
import org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory;
import org.apache.samza.storage.blobstore.Metadata;
import org.apache.samza.storage.blobstore.exceptions.DeletedException;
import org.apache.samza.storage.blobstore.metrics.BlobStoreBackupManagerMetrics;
import org.apache.samza.storage.blobstore.metrics.BlobStoreRestoreManagerMetrics;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.FutureUtil;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContainerStorageManagerRestoreUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerStorageManagerRestoreUtil.class);

  /**
   * Inits and Restores all the task stores.
   * Note: In case of {@link BlobStoreRestoreManager}, this method retries init and restore with getDeleted flag if it
   * receives a {@link DeletedException}. This will create a new checkpoint for the corresponding task.
   */
  public static CompletableFuture<Map<TaskName, Checkpoint>> initAndRestoreTaskInstances(
      Map<TaskName, Map<String, TaskRestoreManager>> taskRestoreManagers, SamzaContainerMetrics samzaContainerMetrics,
      CheckpointManager checkpointManager, JobContext jobContext, ContainerModel containerModel,
      Map<TaskName, Checkpoint> taskCheckpoints, Map<TaskName, Map<String, Set<String>>> taskBackendFactoryToStoreNames,
      Config config, ExecutorService executor, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      File loggerStoreDir, Map<String, SystemConsumer> storeConsumers) {

    Set<String> forceRestoreTasks = new HashSet<>();
    // Initialize each TaskStorageManager.
    taskRestoreManagers.forEach((taskName, restoreManagers) ->
        restoreManagers.forEach((factoryName, taskRestoreManager) -> {
          try {
            taskRestoreManager.init(taskCheckpoints.get(taskName));
          } catch (SamzaException ex) {
            if (isUnwrappedExceptionDeletedException(ex) && taskRestoreManager instanceof BlobStoreRestoreManager) {
              // Get deleted SnapshotIndex blob with GetDeleted and mark the task to be restored with GetDeleted as well.
              // this ensures that the restore downloads the snapshot, recreates a new snapshot, uploads it to blob store
              // and creates a new checkpoint.
              ((BlobStoreRestoreManager) taskRestoreManager).init(taskCheckpoints.get(taskName), true);
              forceRestoreTasks.add(taskName.getTaskName());
            } else {
              // log and rethrow exception to communicate restore failure
              String msg = String.format("init failed for BlobStoreRestoreManager, task: %s with GetDeleted set to true", taskName);
              LOG.error(msg, ex);
              throw new SamzaException(msg, ex);
            }
          }
        })
    );

    // Start each store consumer once.
    // Note: These consumers are per system and only changelog system store consumers will be started.
    // Some TaskRestoreManagers may not require the consumer to be started, but due to the agnostic nature of
    // ContainerStorageManager we always start the changelog consumer here in case it is required
    storeConsumers.values().stream().distinct().forEach(SystemConsumer::start);

    return restoreAllTaskInstances(taskRestoreManagers, taskCheckpoints, taskBackendFactoryToStoreNames, jobContext,
        containerModel, samzaContainerMetrics, checkpointManager, config, taskInstanceMetrics, executor, loggerStoreDir,
        forceRestoreTasks);
  }

  /**
   * Restores all TaskInstances and returns a future for each TaskInstance restore. Note: In case of
   * {@link BlobStoreRestoreManager}, this method restore with getDeleted flag if it receives a
   * {@link DeletedException}. This will create a new Checkpoint.
   */
  private static CompletableFuture<Map<TaskName, Checkpoint>> restoreAllTaskInstances(
      Map<TaskName, Map<String, TaskRestoreManager>> taskRestoreManagers, Map<TaskName, Checkpoint> taskCheckpoints,
      Map<TaskName, Map<String, Set<String>>> taskBackendFactoryToStoreNames, JobContext jobContext,
      ContainerModel containerModel, SamzaContainerMetrics samzaContainerMetrics, CheckpointManager checkpointManager,
      Config config, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics, ExecutorService executor,
      File loggedStoreDir, Set<String> forceRestoreTask) {

    Map<TaskName, CompletableFuture<Checkpoint>> newTaskCheckpoints = new ConcurrentHashMap<>();
    List<Future<Void>> taskRestoreFutures = new ArrayList<>();

    // Submit restore callable for each taskInstance
    taskRestoreManagers.forEach((taskInstanceName, restoreManagersMap) -> {
      // Submit for each restore factory
      restoreManagersMap.forEach((factoryName, taskRestoreManager) -> {
        long startTime = System.currentTimeMillis();
        String taskName = taskInstanceName.getTaskName();
        LOG.info("Starting restore for state for task: {}", taskName);

        CompletableFuture<Void> restoreFuture;
        if (forceRestoreTask.contains(taskName) && taskRestoreManager instanceof BlobStoreRestoreManager) {
          // If init was retried with getDeleted, force restore with getDeleted as well, since init only inits the
          // restoreManager with deleted SnapshotIndex but does not retry to recover the deleted blobs and delegates it
          // to restore().
          // Create an empty future that fails immediately with DeletedException to force retry in restore.
          restoreFuture = new CompletableFuture<>();
          restoreFuture.completeExceptionally(new SamzaException(new DeletedException()));
        } else {
          restoreFuture = taskRestoreManager.restore();
        }

        CompletableFuture<Void> taskRestoreFuture = restoreFuture.handle((res, ex) -> {
          updateRestoreTime(startTime, samzaContainerMetrics, taskInstanceName);

          if (ex != null) {
            if (isUnwrappedExceptionDeletedException(ex)) {
              LOG.warn("Received DeletedException during restore for task {}. Attempting to get blobs with getDeleted flag",
                  taskInstanceName.getTaskName());

              // Try to restore with getDeleted flag
              CompletableFuture<Checkpoint> future =
                  restoreDeletedSnapshot(taskInstanceName, taskCheckpoints,
                      checkpointManager, taskRestoreManager, config, taskInstanceMetrics, executor,
                      taskBackendFactoryToStoreNames.get(taskInstanceName).get(factoryName), loggedStoreDir,
                      jobContext, containerModel);
              try {
                newTaskCheckpoints.put(taskInstanceName, future);
              } catch (Exception e) {
                String msg = String.format("DeletedException during restore task: %s after retrying to get deleted blobs.", taskName);
                throw new SamzaException(msg, e);
              } finally {
                updateRestoreTime(startTime, samzaContainerMetrics, taskInstanceName);
              }
            } else {
              // log and rethrow exception to communicate restore failure
              String msg = String.format("Error restoring state for task: %s", taskName);
              LOG.error(msg, ex);
              throw new SamzaException(msg, ex); // wrap in unchecked exception to throw from lambda
            }
          }

          // Stop all persistent stores after restoring. Certain persistent stores opened in BulkLoad mode are compacted
          // on stop, so paralleling stop() also parallelizes their compaction (a time-intensive operation).
          try {
            taskRestoreManager.close();
          } catch (Exception e) {
            LOG.error("Error closing restore manager for task: {} after {} restore", taskName,
                ex != null ? "unsuccessful" : "successful", e);
            // ignore exception from close. container may still be able to continue processing/backups
            // if restore manager close fails.
          }
          return null;
        });
        taskRestoreFutures.add(taskRestoreFuture);
      });
    });
    CompletableFuture<Void> restoreFutures = CompletableFuture.allOf(taskRestoreFutures.toArray(new CompletableFuture[0]));
    return restoreFutures.thenCompose(ignoredVoid -> FutureUtil.toFutureOfMap(newTaskCheckpoints));
  }

  /**
   * Returns a single future that guarantees all the following are completed, in this order:
   *   1. Restore state locally by getting deleted blobs from the blob store.
   *   2. Create a new snapshot from restored state by backing it up on the blob store.
   *   3. Remove TTL from the new Snapshot and all the associated blobs in the blob store
   *   4. Clean up old/deleted Snapshot
   *   5. Create and write the new checkpoint to checkpoint topic
   */
  private static CompletableFuture<Checkpoint> restoreDeletedSnapshot(TaskName taskName,
      Map<TaskName, Checkpoint> taskCheckpoints, CheckpointManager checkpointManager,
      TaskRestoreManager taskRestoreManager, Config config, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      ExecutorService executor, Set<String> storesToRestore, File loggedStoreBaseDirectory, JobContext jobContext,
      ContainerModel containerModel) {

    // if taskInstanceMetrics are specified use those for store metrics,
    // otherwise (in case of StorageRecovery) use a blank MetricsRegistryMap
    MetricsRegistry metricsRegistry =
        taskInstanceMetrics.get(taskName) != null ? taskInstanceMetrics.get(taskName).registry()
            : new MetricsRegistryMap();

    BlobStoreManager blobStoreManager = getBlobStoreManager(config, executor);
    JobConfig jobConfig = new JobConfig(config);
    BlobStoreUtil blobStoreUtil =
        new BlobStoreUtil(blobStoreManager, executor, new BlobStoreConfig(config), null,
            new BlobStoreRestoreManagerMetrics(metricsRegistry));

    BlobStoreRestoreManager blobStoreRestoreManager = (BlobStoreRestoreManager) taskRestoreManager;

    CheckpointId checkpointId = CheckpointId.create();
    Map<String, String> oldSCMs = ((CheckpointV2) taskCheckpoints.get(taskName)).getStateCheckpointMarkers()
        .get(BlobStoreStateBackendFactory.class.getName());

    // 1. Restore state with getDeleted flag set to true
    CompletableFuture<Void> restoreFuture = blobStoreRestoreManager.restore(true);

    // 2. Create a new checkpoint and back it up on the blob store
    CompletableFuture<Map<String, String>> backupStoresFuture = restoreFuture.thenCompose(
      r -> backupRecoveredStore(jobContext, containerModel, config, taskName, storesToRestore, checkpointId,
          loggedStoreBaseDirectory, blobStoreManager, metricsRegistry, executor));

    // 3. Mark new Snapshots to never expire
    CompletableFuture<Void> removeNewSnapshotsTTLFuture = backupStoresFuture.thenCompose(
      storeSCMs -> {
        List<CompletableFuture<Void>> removeTTLForSnapshotIndexFutures = new ArrayList<>();
        storeSCMs.forEach((store, scm) -> {
          Metadata requestMetadata = new Metadata(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH, Optional.empty(),
              jobConfig.getName().get(), jobConfig.getJobId(), taskName.getTaskName(), store);
          removeTTLForSnapshotIndexFutures.add(blobStoreUtil.removeTTLForSnapshotIndex(scm, requestMetadata));
        });
        return CompletableFuture.allOf(removeTTLForSnapshotIndexFutures.toArray(new CompletableFuture[0]));
      });

    // 4. Delete prev SnapshotIndex including files/subdirs
    CompletableFuture<Void> deleteOldSnapshotsFuture = removeNewSnapshotsTTLFuture.thenCompose(
      ignore -> {
        List<CompletableFuture<Void>> deletePrevSnapshotFutures = new ArrayList<>();
        oldSCMs.forEach((store, oldSCM) -> {
          Metadata requestMetadata = new Metadata(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH, Optional.empty(),
              jobConfig.getName().get(), jobConfig.getJobId(), taskName.getTaskName(), store);
          deletePrevSnapshotFutures.add(blobStoreUtil.cleanSnapshotIndex(oldSCM, requestMetadata, true).toCompletableFuture());
        });
        return CompletableFuture.allOf(deletePrevSnapshotFutures.toArray(new CompletableFuture[0]));
      });

    // 5. create new checkpoint
    CompletableFuture<Checkpoint> newTaskCheckpointsFuture =
        deleteOldSnapshotsFuture.thenCombine(backupStoresFuture, (aVoid, scms) ->
            writeNewCheckpoint(taskName, checkpointId, scms, checkpointManager));

    return newTaskCheckpointsFuture.exceptionally(ex -> {
      String msg = String.format("Could not restore task: %s after attempting to restore deleted blobs.", taskName);
      throw new SamzaException(msg, ex);
    });
  }

  private static CompletableFuture<Map<String, String>> backupRecoveredStore(JobContext jobContext,
      ContainerModel containerModel, Config config, TaskName taskName, Set<String> storesToBackup,
      CheckpointId newCheckpointId, File loggedStoreBaseDirectory, BlobStoreManager blobStoreManager,
      MetricsRegistry metricsRegistry, ExecutorService executor) {

    BlobStoreBackupManagerMetrics blobStoreBackupManagerMetrics = new BlobStoreBackupManagerMetrics(metricsRegistry);
    BlobStoreBackupManager blobStoreBackupManager =
        new BlobStoreBackupManager(jobContext.getJobModel(), containerModel, containerModel.getTasks().get(taskName),
            executor, blobStoreBackupManagerMetrics, config, SystemClock.instance(), loggedStoreBaseDirectory,
            new StorageManagerUtil(), blobStoreManager);

    // create checkpoint dir as a copy of store dir
    createCheckpointDirFromStoreDirCopy(taskName, containerModel.getTasks().get(taskName),
        loggedStoreBaseDirectory, storesToBackup, newCheckpointId);
    // upload to blob store and return future
    return blobStoreBackupManager.upload(newCheckpointId, new HashMap<>());
  }

  private static Checkpoint writeNewCheckpoint(TaskName taskName, CheckpointId checkpointId,
      Map<String, String> uploadSCMs, CheckpointManager checkpointManager) {
    CheckpointV2 oldCheckpoint = (CheckpointV2) checkpointManager.readLastCheckpoint(taskName);
    Map<SystemStreamPartition, String> inputOffsets = oldCheckpoint.getOffsets();

    ImmutableMap.Builder<String, Map<String, String>> newSCMBuilder = ImmutableMap.builder();
    newSCMBuilder.put(BlobStoreStateBackendFactory.class.getName(), uploadSCMs);

    Map<String, String> oldKafkaChangelogStateCheckpointMarkers =
        oldCheckpoint.getStateCheckpointMarkers().get(KafkaChangelogStateBackendFactory.class.getName());
    if (oldKafkaChangelogStateCheckpointMarkers != null) {
      newSCMBuilder.put(KafkaChangelogStateBackendFactory.class.getName(), oldKafkaChangelogStateCheckpointMarkers);
    }

    CheckpointV2 checkpointV2 = new CheckpointV2(checkpointId, inputOffsets, newSCMBuilder.build());
    checkpointManager.writeCheckpoint(taskName, checkpointV2);
    return checkpointV2;
  }

  private static void createCheckpointDirFromStoreDirCopy(TaskName taskName, TaskModel taskModel,
      File loggedStoreBaseDir, Set<String> storeName, CheckpointId checkpointId) {
    StorageManagerUtil storageManagerUtil = new StorageManagerUtil();
    for (String store : storeName) {
      try {
        File storeDirectory =
            storageManagerUtil.getTaskStoreDir(loggedStoreBaseDir, store, taskName, taskModel.getTaskMode());
        File checkpointDir = new File(storageManagerUtil.getStoreCheckpointDir(storeDirectory, checkpointId));
        FileUtils.copyDirectory(storeDirectory, checkpointDir);
      } catch (IOException exception) {
        String msg = String.format("Unable to create a copy of store directory %s into checkpoint dir %s while "
            + "attempting to recover from DeletedException", store, checkpointId);
        throw new SamzaException(msg, exception);
      }
    }
  }

  private static BlobStoreManager getBlobStoreManager(Config config, ExecutorService executor) {
    BlobStoreConfig blobStoreConfig = new BlobStoreConfig(config);
    String blobStoreManagerFactory = blobStoreConfig.getBlobStoreManagerFactory();
    BlobStoreManagerFactory factory = ReflectionUtil.getObj(blobStoreManagerFactory, BlobStoreManagerFactory.class);
    return factory.getRestoreBlobStoreManager(config, executor);
  }

  private static void updateRestoreTime(long startTime, SamzaContainerMetrics samzaContainerMetrics,
      TaskName taskInstance) {
    long timeToRestore = System.currentTimeMillis() - startTime;
    if (samzaContainerMetrics != null) {
      Gauge taskGauge = samzaContainerMetrics.taskStoreRestorationMetrics().getOrDefault(taskInstance, null);

      if (taskGauge != null) {
        taskGauge.set(timeToRestore);
      }
    }
  }

  private static Boolean isUnwrappedExceptionDeletedException(Throwable ex) {
    Throwable unwrappedException = FutureUtil.unwrapExceptions(CompletionException.class,
        FutureUtil.unwrapExceptions(SamzaException.class, ex));
    return unwrappedException instanceof DeletedException;
  }
}
