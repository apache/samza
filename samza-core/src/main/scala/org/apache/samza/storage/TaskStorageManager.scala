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

package org.apache.samza.storage

import java.io._
import java.util

import org.apache.samza.config.StorageConfig
import org.apache.samza.{Partition, SamzaException}
import org.apache.samza.container.TaskName
import org.apache.samza.system._
import org.apache.samza.util.{Clock, FileUtil, Logging}

object TaskStorageManager {
  def getStoreDir(storeBaseDir: File, storeName: String) = {
    new File(storeBaseDir, storeName)
  }

  def getStorePartitionDir(storeBaseDir: File, storeName: String, taskName: TaskName) = {
    // TODO: Sanitize, check and clean taskName string as a valid value for a file
    new File(storeBaseDir, (storeName + File.separator + taskName.toString).replace(' ', '_'))
  }
}

/**
 * Manage all the storage engines for a given task
 */
class TaskStorageManager(
  taskName: TaskName,
  taskStores: Map[String, StorageEngine] = Map(),
  storeConsumers: Map[String, SystemConsumer] = Map(),
  changeLogSystemStreams: Map[String, SystemStream] = Map(),
  changeLogStreamPartitions: Int,
  streamMetadataCache: StreamMetadataCache,
  sspMetadataCache: SSPMetadataCache,
  nonLoggedStoreBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  loggedStoreBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  partition: Partition,
  systemAdmins: SystemAdmins,
  changeLogDeleteRetentionsInMs: Map[String, Long],
  clock: Clock) extends Logging {

  var taskStoresToRestore = taskStores.filter{
    case (storeName, storageEngine) => storageEngine.getStoreProperties.isLoggedStore
  }
  val persistedStores = taskStores.filter{
    case (storeName, storageEngine) => storageEngine.getStoreProperties.isPersistedToDisk
  }

  var changeLogOldestOffsets: Map[SystemStream, String] = Map()
  val fileOffsets: util.Map[SystemStreamPartition, String] = new util.HashMap[SystemStreamPartition, String]()
  val offsetFileName = "OFFSET"

  def getStore(storeName: String): Option[StorageEngine] = taskStores.get(storeName)

  def init {
    cleanBaseDirs()
    setupBaseDirs()
    validateChangelogStreams()
    startConsumers()
    restoreStores()
    stopConsumers()
  }

  private def cleanBaseDirs() {
    debug("Cleaning base directories for stores.")

    taskStores.keys.foreach(storeName => {
      val nonLoggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(nonLoggedStoreBaseDir, storeName, taskName)
      info("Got non logged storage partition directory as %s" format nonLoggedStorePartitionDir.toPath.toString)

      if(nonLoggedStorePartitionDir.exists()) {
        info("Deleting non logged storage partition directory %s" format nonLoggedStorePartitionDir.toPath.toString)
        FileUtil.rm(nonLoggedStorePartitionDir)
      }

      val loggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
      info("Got logged storage partition directory as %s" format loggedStorePartitionDir.toPath.toString)

      // Delete the logged store if it is not valid.
      if (!isLoggedStoreValid(storeName, loggedStorePartitionDir)) {
        info("Deleting logged storage partition directory %s." format loggedStorePartitionDir.toPath.toString)
        FileUtil.rm(loggedStorePartitionDir)
      } else {
        val offset = StorageManagerUtil.readOffsetFile(loggedStorePartitionDir, offsetFileName)
        info("Read offset %s for the store %s from logged storage partition directory %s." format(offset, storeName, loggedStorePartitionDir))
        if (offset != null) {
          fileOffsets.put(new SystemStreamPartition(changeLogSystemStreams(storeName), partition), offset)
        }
      }
    })
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
  private def isLoggedStoreValid(storeName: String, loggedStoreDir: File): Boolean = {
    val changeLogDeleteRetentionInMs = changeLogDeleteRetentionsInMs
      .getOrElse(storeName, StorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS)

    persistedStores.contains(storeName) &&
      StorageManagerUtil.isOffsetFileValid(loggedStoreDir, offsetFileName) &&
      !StorageManagerUtil.isStaleStore(loggedStoreDir, offsetFileName, changeLogDeleteRetentionInMs, clock.currentTimeMillis())
  }

  private def setupBaseDirs() {
    debug("Setting up base directories for stores.")
    taskStores.foreach {
      case (storeName, storageEngine) =>
        if (storageEngine.getStoreProperties.isLoggedStore) {
          val loggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
          info("Using logged storage partition directory: %s for store: %s." format(loggedStorePartitionDir.toPath.toString, storeName))
          if (!loggedStorePartitionDir.exists()) loggedStorePartitionDir.mkdirs()
        } else {
          val nonLoggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(nonLoggedStoreBaseDir, storeName, taskName)
          info("Using non logged storage partition directory: %s for store: %s." format(nonLoggedStorePartitionDir.toPath.toString, storeName))
          nonLoggedStorePartitionDir.mkdirs()
        }
    }
  }

  private def validateChangelogStreams() = {
    info("Validating change log streams: " + changeLogSystemStreams)

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemAdmin = systemAdmins.getSystemAdmin(systemStream.getSystem)
      val changelogSpec = StreamSpec.createChangeLogStreamSpec(systemStream.getStream, systemStream.getSystem, changeLogStreamPartitions)

      systemAdmin.validateStream(changelogSpec)
    }

    val changeLogMetadata = streamMetadataCache.getStreamMetadata(changeLogSystemStreams.values.toSet)
    info("Got change log stream metadata: %s" format changeLogMetadata)

    changeLogOldestOffsets = getChangeLogOldestOffsetsForPartition(partition, changeLogMetadata)
    info("Assigning oldest change log offsets for taskName %s: %s" format (taskName, changeLogOldestOffsets))
  }

  private def startConsumers() {
    debug("Starting consumers for stores.")

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
      val admin = systemAdmins.getSystemAdmin(systemStream.getSystem)
      val consumer = storeConsumers(storeName)

      val offset = getStartingOffset(systemStreamPartition, admin)
      if (offset != null) {
        info("Registering change log consumer with offset %s for %s." format (offset, systemStreamPartition))
        consumer.register(systemStreamPartition, offset)
      } else {
        info("Skipping change log restoration for %s because stream appears to be empty (offset was null)." format systemStreamPartition)
        taskStoresToRestore -= storeName
      }
    }

    storeConsumers.values.foreach(_.start)
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
    * @param admin                  the [[SystemAdmin]] for the changelog.
    * @return                       the offset to from which the changelog consumer should be initialized.
    */
  private def getStartingOffset(systemStreamPartition: SystemStreamPartition, admin: SystemAdmin) = {
    val fileOffset = fileOffsets.get(systemStreamPartition)
    val oldestOffset = changeLogOldestOffsets
      .getOrElse(systemStreamPartition.getSystemStream,
        throw new SamzaException("Missing a change log offset for %s." format systemStreamPartition))

    StorageManagerUtil.getStartingOffset(systemStreamPartition, admin, fileOffset, oldestOffset)
  }

  private def restoreStores() {
    debug("Restoring stores.")

    for ((storeName, store) <- taskStoresToRestore) {
      if (changeLogSystemStreams.contains(storeName)) {
        val systemStream = changeLogSystemStreams(storeName)
        val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
        val systemConsumer = storeConsumers(storeName)
        val systemConsumerIterator = new SystemStreamPartitionIterator(systemConsumer, systemStreamPartition)
        store.restore(systemConsumerIterator)
      }
    }
  }

  private def stopConsumers() {
    debug("Stopping consumers for stores.")

    storeConsumers.values.foreach(_.stop)
  }

  def flush() {
    debug("Flushing stores.")

    taskStores.values.foreach(_.flush)
    flushChangelogOffsetFiles()
  }

  def stopStores() {
    debug("Stopping stores.")
    taskStores.values.foreach(_.stop)
  }

  def stop() {
    stopStores()

    flushChangelogOffsetFiles()
  }

  /**
    * Writes the offset files for each changelog to disk.
    * These files are used when stores are restored from disk to determine whether
    * there is any new information in the changelog that is not reflected in the disk
    * copy of the store. If there is any delta, it is replayed from the changelog
    * e.g. This can happen if the job was run on this host, then another
    * host and back to this host.
    */
  private def flushChangelogOffsetFiles() {
    debug("Persisting logged key value stores")

    for ((storeName, systemStream) <- changeLogSystemStreams.filterKeys(storeName => persistedStores.contains(storeName))) {
      debug("Fetching newest offset for store %s" format(storeName))
      try {
        val ssp = new SystemStreamPartition(systemStream.getSystem, systemStream.getStream, partition)
        val sspMetadata = sspMetadataCache.getMetadata(ssp)
        val newestOffset = if (sspMetadata == null) null else sspMetadata.getNewestOffset
        debug("Got offset %s for store %s" format(newestOffset, storeName))

        val loggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
        val offsetFile = new File(loggedStorePartitionDir, offsetFileName)
        if (newestOffset != null) {
          debug("Storing offset for store in OFFSET file ")
          FileUtil.writeWithChecksum(offsetFile, newestOffset)
          debug("Successfully stored offset %s for store %s in OFFSET file " format(newestOffset, storeName))
        } else {
          //if newestOffset is null, then it means the store is (or has become) empty. No need to persist the offset file
          if (offsetFile.exists()) {
            FileUtil.rm(offsetFile)
          }
          debug("Not storing OFFSET file for taskName %s. Store %s backed by changelog topic: %s, partition: %s is empty. " format (taskName, storeName, systemStream.getStream, partition.getPartitionId))
        }
      } catch {
        case e: Exception => error("Exception storing offset for store %s. Skipping." format(storeName), e)
      }

    }

    debug("Done persisting logged key value stores")
  }

  /**
   * Builds a map from SystemStreamPartition to oldest offset for changelogs.
   */
  private def getChangeLogOldestOffsetsForPartition(partition: Partition, inputStreamMetadata: Map[SystemStream, SystemStreamMetadata]): Map[SystemStream, String] = {
    inputStreamMetadata
      .mapValues(_.getSystemStreamPartitionMetadata.get(partition))
      .filter(_._2 != null)
      .mapValues(_.getOldestOffset)
  }
}
