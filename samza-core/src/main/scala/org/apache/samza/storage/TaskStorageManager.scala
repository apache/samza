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

import com.google.common.annotations.VisibleForTesting
import org.apache.samza.Partition
import org.apache.samza.container.TaskName
import org.apache.samza.job.model.TaskMode
import org.apache.samza.system._
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.util.{FileUtil, Logging}

import scala.collection.JavaConverters._

/**
 * Manage all the storage engines for a given task
 */
class TaskStorageManager(
  taskName: TaskName,
  containerStorageManager: ContainerStorageManager,
  changeLogSystemStreams: Map[String, SystemStream] = Map(),
  sspMetadataCache: SSPMetadataCache,
  loggedStoreBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  partition: Partition) extends Logging {

  val persistedStores = containerStorageManager.getAllStores(taskName).asScala.filter{
    case (storeName, storageEngine) => storageEngine.getStoreProperties.isPersistedToDisk
  }

  def getStore(storeName: String): Option[StorageEngine] =  JavaOptionals.toRichOptional(containerStorageManager.getStore(taskName, storeName)).toOption

  def init {
  }

  def flush() {
    debug("Flushing stores.")

    containerStorageManager.getAllStores(taskName).asScala.values.foreach(_.flush)
    flushChangelogOffsetFiles()
  }

  def stopStores() {
    debug("Stopping stores.")
    containerStorageManager.stopStores();
  }

  @VisibleForTesting
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

        if (newestOffset != null) {
          debug("Storing offset for store in OFFSET file ")

          // TaskStorageManagers are only spun-up for active tasks
          StorageManagerUtil.writeOffsetFile(loggedStoreBaseDir, storeName, taskName, TaskMode.Active, Map(ssp -> newestOffset).asJava)
          debug("Successfully stored offset %s for store %s in OFFSET file " format(newestOffset, storeName))
        } else {
          //if newestOffset is null, then it means the store is (or has become) empty. No need to persist the offset file
          StorageManagerUtil.deleteOffsetFile(loggedStoreBaseDir, storeName, taskName);
          debug("Not storing OFFSET file for taskName %s. Store %s backed by changelog topic: %s, partition: %s is empty. " format (taskName, storeName, systemStream.getStream, partition.getPartitionId))
        }
      } catch {
        case e: Exception => error("Exception storing offset for store %s. Skipping." format(storeName), e)
      }

    }

    debug("Done persisting logged key value stores")
  }
}
