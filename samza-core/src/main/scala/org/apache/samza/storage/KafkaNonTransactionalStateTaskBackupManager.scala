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

import java.util
import java.util.concurrent.CompletableFuture

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
class KafkaNonTransactionalStateTaskBackupManager(
  taskName: TaskName,
  storeChangelogs: util.Map[String, SystemStream] = new util.HashMap[String, SystemStream](),
  systemAdmins: SystemAdmins,
  partition: Partition) extends Logging with TaskBackupManager {

  override def init(checkpoint: Checkpoint): Unit = {}

  override def snapshot(checkpointId: CheckpointId): util.Map[String, String] = {
    debug("Getting newest offsets for kafka changelog SSPs.")
    getNewestChangelogSSPOffsets()
  }

  override def upload(checkpointId: CheckpointId,
    stateCheckpointMarkers: util.Map[String, String]): CompletableFuture[util.Map[String, String]] = {
     CompletableFuture.completedFuture(stateCheckpointMarkers)
  }

  override def cleanUp(checkpointId: CheckpointId,
    stateCheckpointMarker: util.Map[String, String]): CompletableFuture[Void] = {
    CompletableFuture.completedFuture(null)
  }

  override def close() {}

  /**
   * Returns the newest offset for each store changelog SSP for this task.
   * @return A map of changelog SSPs for this task to their newest offset (or None if ssp is empty)
   * @throws SamzaException if there was an error fetching newest offset for any SSP
   */
  private def getNewestChangelogSSPOffsets(): util.Map[String, String] = {
    storeChangelogs.asScala
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

          (storeName, new KafkaStateCheckpointMarker(ssp, newestOffsetOption.orNull).toString)
        } catch {
          case e: Exception =>
            throw new SamzaException("Error getting newest changelog offset for taskName %s store %s changelog %s."
              format(taskName, storeName, systemStream), e)
        }
      }}.asJava
  }
}
