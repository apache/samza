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

import java.io.File
import scala.collection.Map
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
    new File(storeBaseDir, storeName + File.separator + taskName)
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
  partition: Partition,
  systemAdmins: Map[String, SystemAdmin]) extends Logging {

  var taskStoresToRestore = taskStores
  var changeLogOldestOffsets: Map[SystemStream, String] = Map()

  def apply(storageEngineName: String) = taskStores(storageEngineName)

  def init {
    cleanBaseDirs
    createStreams
    startConsumers
    restoreStores
    stopConsumers
  }

  private def cleanBaseDirs {
    debug("Cleaning base directories for stores.")
    taskStores.keys.foreach(storeName => {
      val storagePartitionDir = TaskStorageManager.getStorePartitionDir(storeBaseDir, storeName, taskName)

      debug("Cleaning %s for store %s." format (storagePartitionDir, storeName))

      Util.rm(storagePartitionDir)
      storagePartitionDir.mkdirs
    })
  }

  private def createStreams = {
    info("Creating streams that are not present for changelog")

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      var systemAdmin = systemAdmins.getOrElse(systemStream.getSystem, throw new SamzaException("Unable to get systemAdmin for store " + storeName + " and systemStream" + systemStream))
      systemAdmin.createChangelogStream(systemStream.getStream, changeLogStreamPartitions)
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
      val offset = changeLogOldestOffsets.getOrElse(systemStream, throw new SamzaException("Missing a change log offset for %s." format systemStreamPartition))

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
        val systemConsumerIterator = new SystemStreamPartitionIterator(systemConsumer, systemStreamPartition);
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
  }

  def stop() {
    debug("Stopping stores.")

    taskStores.values.foreach(_.stop)
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
