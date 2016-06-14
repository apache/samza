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
import scala.collection.{JavaConversions, Map}
import org.apache.samza.util.Logging
import org.apache.samza.Partition
import org.apache.samza.system._
import org.apache.samza.util.Util
import org.apache.samza.SamzaException
import org.apache.samza.container.TaskName

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
  storeBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  loggedStoreBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  partition: Partition,
  systemAdmins: Map[String, SystemAdmin]) extends Logging {

  var taskStoresToRestore = taskStores
  var changeLogOldestOffsets: Map[SystemStream, String] = Map()
  val fileOffset: util.Map[SystemStreamPartition, String] = new util.HashMap[SystemStreamPartition, String]()
  val offsetFileName = "OFFSET"

  def apply(storageEngineName: String) = taskStores(storageEngineName)

  def init {
    cleanBaseDirs
    setupBaseDirs
    validateChangelogStreams
    startConsumers
    restoreStores
    stopConsumers
  }

  private def setupBaseDirs {
    debug("Setting up base directories for stores.")

    val loggedStores = changeLogSystemStreams.keySet

    (taskStores.keySet -- loggedStores)
      .foreach(TaskStorageManager.getStorePartitionDir(storeBaseDir, _, taskName).mkdirs)

    loggedStores.foreach(storeName => {
      val loggedStoragePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
      if(!loggedStoragePartitionDir.exists()) loggedStoragePartitionDir.mkdirs
    })
  }

  private def cleanBaseDirs {
    debug("Cleaning base directories for stores.")

    taskStores.keys.foreach(storeName => {
      val storagePartitionDir = TaskStorageManager.getStorePartitionDir(storeBaseDir, storeName, taskName)
      info("Got storage partition directory as %s" format storagePartitionDir.toPath.toString)

      if(storagePartitionDir.exists()) {
        debug("Deleting default storage partition directory %s" format storagePartitionDir.toPath.toString)
        Util.rm(storagePartitionDir)
      }

      val loggedStoragePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
      info("Got logged storage partition directory as %s" format loggedStoragePartitionDir.toPath.toString)

      // If we find valid offsets s.t. we can restore the state, keep the disk files. Otherwise, delete them.
      if(!readOffsetFile(storeName, loggedStoragePartitionDir) && loggedStoragePartitionDir.exists()) {
          Util.rm(loggedStoragePartitionDir)
      }
    })
  }

  /**
    * Attempts to read the offset file and returns {@code true} if the offsets were read successfully.
    *
    * @param storeName                  the name of the store for which the offsets are needed
    * @param loggedStoragePartitionDir  the directory for the store
    * @return                           true if the offsets were read successfully, false otherwise.
    */
  private def readOffsetFile(storeName: String, loggedStoragePartitionDir: File): Boolean = {
    var offsetsRead = false
    val offsetFileRef = new File(loggedStoragePartitionDir, offsetFileName)
    if(offsetFileRef.exists()) {
      debug("Found offset file in partition directory: %s" format offsetFileRef.toPath.toString)
      val offset = Util.readDataFromFile(offsetFileRef)
      if(offset != null && !offset.isEmpty) {
        fileOffset.put(new SystemStreamPartition(changeLogSystemStreams(storeName), partition), offset)
        offsetsRead = true
      }
    } else {
      info("No offset file found in logged storage partition directory: %s" format loggedStoragePartitionDir.toPath.toString)
    }
    offsetsRead
  }

  private def validateChangelogStreams = {
    info("Validating change log streams")

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemAdmin = systemAdmins
        .getOrElse(systemStream.getSystem,
                   throw new SamzaException("Unable to get systemAdmin for store " + storeName + " and systemStream" + systemStream))
      systemAdmin.validateChangelogStream(systemStream.getStream, changeLogStreamPartitions)
    }

    val changeLogMetadata = streamMetadataCache.getStreamMetadata(changeLogSystemStreams.values.toSet)
    info("Got change log stream metadata: %s" format changeLogMetadata)

    changeLogOldestOffsets = getChangeLogOldestOffsetsForPartition(partition, changeLogMetadata)
    info("Assigning oldest change log offsets for taskName %s: %s" format (taskName, changeLogOldestOffsets))
  }

  private def startConsumers {
    debug("Starting consumers for stores.")

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
      val consumer = storeConsumers(storeName)
      val offset =
        Option(fileOffset.get(systemStreamPartition))
          .getOrElse(changeLogOldestOffsets
            .getOrElse(systemStream, throw new SamzaException("Missing a change log offset for %s." format systemStreamPartition)))

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

  private def restoreStores {
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

  private def stopConsumers {
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

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemAdmin = systemAdmins
              .getOrElse(systemStream.getSystem,
                         throw new SamzaException("Unable to get systemAdmin for store " + storeName + " and systemStream" + systemStream))

      debug("Fetching newest offset for store %s" format(storeName))
      try {
        val newestOffset = if (systemAdmin.isInstanceOf[ExtendedSystemAdmin]) {
          // This approach is much more efficient because it only fetches the newest offset for 1 SSP
          // rather than newest and oldest offsets for all SSPs. Use it if we can.
          systemAdmin.asInstanceOf[ExtendedSystemAdmin].getNewestOffset(new SystemStreamPartition(systemStream.getSystem, systemStream.getStream, partition), 3)
        } else {
          val streamToMetadata = systemAdmins(systemStream.getSystem)
                  .getSystemStreamMetadata(JavaConversions.setAsJavaSet(Set(systemStream.getStream)))
          val sspMetadata = streamToMetadata
                  .get(systemStream.getStream)
                  .getSystemStreamPartitionMetadata
                  .get(partition)
          sspMetadata.getNewestOffset
        }
        debug("Got offset %s for store %s" format(newestOffset, storeName))

        val offsetFile = new File(TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName), offsetFileName)
        if (newestOffset != null) {
          debug("Storing offset for store in OFFSET file ")
          Util.writeDataToFile(offsetFile, newestOffset)
          debug("Successfully stored offset %s for store %s in OFFSET file " format(newestOffset, storeName))
        } else {
          //if newestOffset is null, then it means the store is (or has become) empty. No need to persist the offset file
          if (offsetFile.exists()) {
            Util.rm(offsetFile)
          }
          debug("Not storing OFFSET file for taskName %s. Store %s backed by changelog topic : %s, partition: %s is empty. " format (taskName, storeName, systemStream.getStream, partition.getPartitionId))
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
