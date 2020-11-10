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
import com.google.common.collect.ImmutableSet
import org.apache.samza.checkpoint.CheckpointId
import org.apache.samza.container.TaskName
import org.apache.samza.job.model.TaskMode
import org.apache.samza.system._
import org.apache.samza.util.Logging
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.{Partition, SamzaException}

import scala.collection.JavaConverters._

/**
 * Manage all the storage engines for a given task
 */
class KafkaNonTransactionalStateTaskStorageBackupManager(
  taskName: TaskName,
  containerStorageManager: ContainerStorageManager,
  storeChangelogs: Map[String, SystemStream] = Map(),
  systemAdmins: SystemAdmins,
  loggedStoreBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  partition: Partition) extends Logging with TaskStorageBackupManager {

  private val storageManagerUtil = new StorageManagerUtil
  private val persistedStores = containerStorageManager.getAllStores(taskName).asScala
    .filter { case (storeName, storageEngine) => storageEngine.getStoreProperties.isPersistedToDisk }

  override def snapshot(): Map[SystemStreamPartition, Option[String]] = {
    debug("Flushing stores.")
    containerStorageManager.getAllStores(taskName).asScala.values.foreach(_.flush)
    val newestChangelogSSPOffsets = getNewestChangelogSSPOffsets()
    writeChangelogOffsetFiles(newestChangelogSSPOffsets)
    newestChangelogSSPOffsets
  }

  override def upload(snapshotCheckpointsMap: Map[SystemStreamPartition, Option[String]]): Map[SystemStreamPartition, Option[String]] = {
    snapshotCheckpointsMap
  }

  override def checkpoint(checkpointId: CheckpointId,
    newestChangelogOffsets: Map[SystemStreamPartition, Option[String]]): Unit = {}

  override def cleanUp(checkpointId: CheckpointId): Unit = {}

  @VisibleForTesting
  def stop() {
    debug("Stopping stores.")
    containerStorageManager.stopStores()
  }

  /**
   * Returns the newest offset for each store changelog SSP for this task.
   * @return A map of changelog SSPs for this task to their newest offset (or None if ssp is empty)
   * @throws SamzaException if there was an error fetching newest offset for any SSP
   */
  private def getNewestChangelogSSPOffsets(): Map[SystemStreamPartition, Option[String]] = {
    storeChangelogs
      .map { case (storeName, systemStream) => {
        debug("Fetching newest offset for taskName %s store %s changelog %s" format (taskName, storeName, systemStream))
        val ssp = new SystemStreamPartition(systemStream.getSystem, systemStream.getStream, partition)
        val systemAdmin = systemAdmins.getSystemAdmin(systemStream.getSystem)

        try {
          val sspMetadataOption = Option(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp)).get(ssp))

          // newest offset == null implies topic is empty
          val newestOffsetOption = sspMetadataOption.flatMap(sspMetadata => Option(sspMetadata.getNewestOffset))
          newestOffsetOption.foreach(newestOffset =>
            debug("Got newest offset %s for taskName %s store %s changelog %s" format(newestOffset, taskName, storeName, systemStream)))

          (ssp, newestOffsetOption)
        } catch {
          case e: Exception =>
            throw new SamzaException("Error getting newest changelog offset for taskName %s store %s changelog %s."
              format(taskName, storeName, systemStream), e)
        }
      }}
  }

  /**
   * Writes the newest changelog ssp offset for each persistent store to the OFFSET file on disk.
   * These files are used during container startup to determine whether there is any new information in the
   * changelog that is not reflected in the on-disk copy of the store. If there is any delta, it is replayed
   * from the changelog e.g. This can happen if the job was run on this host, then another
   * host and back to this host.
   */
  private def writeChangelogOffsetFiles(newestChangelogOffsets: Map[SystemStreamPartition, Option[String]]) {
    debug("Writing OFFSET files for logged persistent key value stores for task %s." format(taskName))

    storeChangelogs
      .filterKeys(storeName => persistedStores.contains(storeName))
      .foreach { case (storeName, systemStream) => {
        debug("Writing changelog offset for taskName %s store %s changelog %s." format(taskName, storeName, systemStream))
        val currentStoreDir = storageManagerUtil.getTaskStoreDir(loggedStoreBaseDir, storeName, taskName, TaskMode.Active)
        try {
          val ssp = new SystemStreamPartition(systemStream.getSystem, systemStream.getStream, partition)
          newestChangelogOffsets(ssp) match {
            case Some(newestOffset) => {
              debug("Storing newest offset %s for taskName %s store %s changelog %s in OFFSET file."
                format(newestOffset, taskName, storeName, systemStream))
              // TaskStorageManagers are only created for active tasks
              storageManagerUtil.writeOffsetFile(currentStoreDir, Map(ssp -> newestOffset).asJava, false)
              debug("Successfully stored offset %s for taskName %s store %s changelog %s in OFFSET file."
                format(newestOffset, taskName, storeName, systemStream))
            }
            case None => {
              // if newestOffset is null, then it means the changelog ssp is (or has become) empty. This could be
              // either because the changelog topic was newly added, repartitioned, or manually deleted and recreated.
              // No need to persist the offset file.
              storageManagerUtil.deleteOffsetFile(currentStoreDir)
              debug("Deleting OFFSET file for taskName %s store %s changelog ssp %s since the newestOffset is null."
                format (taskName, storeName, ssp))
            }
          }
        } catch {
          case e: Exception =>
            throw new SamzaException("Error storing offset for taskName %s store %s changelog %s."
              format(taskName, storeName, systemStream), e)
        }
      }}
    debug("Done writing OFFSET files for logged persistent key value stores for task %s" format(taskName))
  }
}
