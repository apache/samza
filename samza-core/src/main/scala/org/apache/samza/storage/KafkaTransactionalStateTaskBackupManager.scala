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

import java.util
import java.util.concurrent.CompletableFuture

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableSet
import org.apache.samza.checkpoint.kafka.KafkaStateCheckpointMarker
import org.apache.samza.checkpoint.{Checkpoint, CheckpointId}
import org.apache.samza.container.TaskName
import org.apache.samza.system._
import org.apache.samza.util.Logging
import org.apache.samza.{Partition, SamzaException}

import scala.collection.JavaConverters._

/**
 * Manage all the storage engines for a given task
 */
class KafkaTransactionalStateTaskBackupManager(
  taskName: TaskName,
  storeChangelogs: util.Map[String, SystemStream] = new util.HashMap[String, SystemStream](),
  systemAdmins: SystemAdmins,
  partition: Partition) extends Logging with TaskBackupManager {

  override def init(checkpoint: Checkpoint): Unit = {}

  override def snapshot(checkpointId: CheckpointId): util.Map[String, String] = {
    debug("Getting newest offsets for kafka changelog SSPs.")
    getNewestChangelogSSPOffsets(taskName, storeChangelogs, partition, systemAdmins)
  }

  override def upload(checkpointId: CheckpointId, snapshotCheckpointsMap: util.Map[String, String]):
  CompletableFuture[util.Map[String, String]] = {
    CompletableFuture.completedFuture(snapshotCheckpointsMap)
  }

  override def cleanUp(checkpointId: CheckpointId,
    stateCheckpointMarker: util.Map[String, String]): CompletableFuture[Void] = {
    CompletableFuture.completedFuture(null)
  }

  override def close() {}

  /**
   * Returns the newest offset for each store changelog SSP for this task. Returned map will
   * always contain an entry for every changelog SSP.
   * @return A map of storenames for this task to their ssp and newest offset (null if empty) wrapped in KafkaStateCheckpointMarker
   * @throws SamzaException if there was an error fetching newest offset for any SSP
   */
  @VisibleForTesting
  def getNewestChangelogSSPOffsets(taskName: TaskName, storeChangelogs: util.Map[String, SystemStream],
      partition: Partition, systemAdmins: SystemAdmins): util.Map[String, String] = {
    storeChangelogs.asScala
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

          (storeName, new KafkaStateCheckpointMarker(ssp, newestOffsetOption.orNull).toString)
        } catch {
          case e: Exception =>
            throw new SamzaException("Error getting newest changelog offset for taskName %s store %s changelog %s."
              format(taskName, storeName, systemStream), e)
        }
      }}
      .toMap.asJava
  }
}
