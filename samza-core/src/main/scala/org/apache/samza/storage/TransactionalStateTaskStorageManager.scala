package org.apache.samza.storage

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

import java.io._
import java.nio.file.Path

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableSet
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.samza.{Partition, SamzaException}
import org.apache.samza.container.TaskName
import org.apache.samza.job.model.TaskMode
import org.apache.samza.system._
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

/**
 * Manage all the storage engines for a given task
 */
class TransactionalStateTaskStorageManager(
  taskName: TaskName,
  containerStorageManager: ContainerStorageManager,
  storeChangelogs: Map[String, SystemStream] = Map(),
  systemAdmins: SystemAdmins,
  loggedStoreBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  partition: Partition,
  taskMode: TaskMode) extends Logging with TaskStorageManager {

  private val storageManagerUtil = new StorageManagerUtil

  def getStore(storeName: String): Option[StorageEngine] =  JavaOptionals.toRichOptional(containerStorageManager.getStore(taskName, storeName)).toOption

  def flush(): Map[SystemStreamPartition, Option[String]] = {
    debug("Flushing stores.")
    containerStorageManager.getAllStores(taskName).asScala.values.foreach(_.flush)
    getNewestChangelogSSPOffsets(taskName, storeChangelogs, partition, systemAdmins)
  }

  def checkpoint(newestChangelogOffsets: Map[SystemStreamPartition, Option[String]]): String = {
    debug("Checkpointing stores.")

    val id = System.currentTimeMillis().toString
    val checkpointPaths = containerStorageManager.getAllStores(taskName).asScala
      .filter { case (storeName, storeEngine) =>
        storeEngine.getStoreProperties.isLoggedStore && storeEngine.getStoreProperties.isPersistedToDisk}
      .flatMap { case (storeName, storeEngine) => {
        val pathOptional = storeEngine.checkpoint(id)
        if (pathOptional.isPresent) {
          Some(storeName, pathOptional.get())
        } else {
          None
        }
      }}
      .toMap

    writeChangelogOffsetFiles(checkpointPaths, storeChangelogs, newestChangelogOffsets)

    id
  }

  def removeOldCheckpoints(latestCheckpointId: String): Unit = {
    if (latestCheckpointId != null) {
      debug("Remove older checkpoints before " + latestCheckpointId)

      val files = loggedStoreBaseDir.listFiles()
      if (files != null) {
        files
          .foreach(storeDir => {
            val storeName = storeDir.getName
            val taskStoreName = storageManagerUtil.getTaskStoreDir(
              loggedStoreBaseDir, storeName, taskName, taskMode).getName
            val fileFilter: FileFilter = new WildcardFileFilter(taskStoreName + "-*")
            val checkpointDirs = storeDir.listFiles(fileFilter)

            checkpointDirs
              .filter(!_.getName.contains(latestCheckpointId))
              .foreach(checkpointDir => {
                FileUtils.deleteDirectory(checkpointDir)
              })
          })
      }
    }
  }

  @VisibleForTesting
  def stop() {
    debug("Stopping stores.")
    containerStorageManager.stopStores()
  }

  /**
   * Returns the newest offset for each store changelog SSP for this task. Returned map will
   * always contain an entry for every changelog SSP.
   * @return A map of changelog SSPs for this task to their newest offset (or None if ssp is empty)
   * @throws SamzaException if there was an error fetching newest offset for any SSP
   */
  @VisibleForTesting
  def getNewestChangelogSSPOffsets(taskName: TaskName, storeChangelogs: Map[String, SystemStream],
      partition: Partition, systemAdmins: SystemAdmins): Map[SystemStreamPartition, Option[String]] = {
    storeChangelogs
      .map { case (storeName, systemStream) => {
        try {
          debug("Fetching newest offset for taskName %s store %s changelog %s" format (taskName, storeName, systemStream))
          val ssp = new SystemStreamPartition(systemStream.getSystem, systemStream.getStream, partition)
          val systemAdmin = systemAdmins.getSystemAdmin(systemStream.getSystem)

          val sspMetadata = Option(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp)).get(ssp))
            .getOrElse(throw new SamzaException("Received null metadata for ssp: %s" format ssp))

          // newest offset == null implies topic is empty
          val newestOffsetOption = Option(sspMetadata.getNewestOffset)
          newestOffsetOption.foreach(newestOffset =>
            debug("Got newest offset %s for taskName %s store %s changelog %s" format(newestOffset, taskName, storeName, systemStream)))

          (ssp, newestOffsetOption)
        } catch {
          case e: Exception =>
            throw new SamzaException("Error getting newest changelog offset for taskName %s store %s changelog %s."
              format(taskName, storeName, systemStream), e)
        }
      }}
      .toMap
  }

  /**
   * Writes the newest changelog ssp offset for each persistent store the OFFSET file in the checkpoint directory.
   * These files are used during container startup to ensure transactional state, and to determine whether the
   * there is any new information in the changelog that is not reflected in the on-disk copy of the store.
   * If there is any delta, it is replayed from the changelog e.g. This can happen if the job was run on this host,
   * then another host, and then back to this host.
   */
  @VisibleForTesting
  def writeChangelogOffsetFiles(checkpointPaths: Map[String, Path], storeChangelogs: Map[String, SystemStream], newestChangelogOffsets: Map[SystemStreamPartition, Option[String]]): Unit = {
    debug("Writing OFFSET files for logged persistent key value stores for task %s." format(checkpointPaths))

    storeChangelogs
      .filterKeys(storeName => checkpointPaths.contains(storeName))
      .foreach { case (storeName, systemStream) => {
        try {
          val ssp = new SystemStreamPartition(systemStream.getSystem, systemStream.getStream, partition)
          newestChangelogOffsets(ssp).foreach(newestOffset => {
            val path = checkpointPaths(storeName)
            debug("Storing newest offset: %s for taskName: %s store: %s changelog: %s in OFFSET file at path: %s."
              format(newestOffset, taskName, storeName, systemStream, path))
            storageManagerUtil.writeOffsetFile(path.toFile, Map(ssp -> newestOffset).asJava, false)
            debug("Successfully stored offset: %s for taskName: %s store: %s changelog: %s in OFFSET file at path: %s."
              format(newestOffset, taskName, storeName, systemStream, path))
          })
        } catch {
          case e: Exception =>
            throw new SamzaException("Error storing offset for taskName %s store %s changelog %s."
              format(taskName, storeName, systemStream), e)
        }
      }}
    debug("Done writing OFFSET files for logged persistent key value stores for task %s" format(taskName))
  }
}
