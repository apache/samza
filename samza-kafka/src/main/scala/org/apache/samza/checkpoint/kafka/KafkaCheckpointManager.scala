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

package org.apache.samza.checkpoint.kafka

import java.util.Collections

import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.config.{Config, JobConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.serializers.Serde
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.kafka.KafkaStreamSpec
import org.apache.samza.system._
import org.apache.samza.util._
import org.apache.samza.{Partition, SamzaException}

import scala.collection.mutable

/**
  * A [[CheckpointManager]] that uses a Kafka topic-partition to store the [[Checkpoint]] corresponding to
  * a task.
  *
  * <p> The Kafka partition provides an abstraction of a log to which all [[Checkpoint]]s are appended to. The
  * checkpoints written to the log are keyed by their corresponding taskName.
  *
  * <p> This class is thread safe for writing but not for reading checkpoints. This is currently OK since checkpoints
  * are only read on the main thread.
  */
class KafkaCheckpointManager(checkpointSpec: KafkaStreamSpec,
  systemFactory: SystemFactory,
  failOnCheckpointValidation: Boolean,
  config: Config,
  metricsRegistry: MetricsRegistry,
  checkpointMsgSerde: Serde[Checkpoint] = new CheckpointSerde) extends CheckpointManager with Logging {

  val checkpointSystem: String = checkpointSpec.getSystemName
  val checkpointTopic: String = checkpointSpec.getPhysicalName
  val checkpointSsp: SystemStreamPartition = new SystemStreamPartition(checkpointSystem, checkpointTopic, new Partition(0))
  val checkpointKeySerde: KafkaCheckpointLogKeySerde = new KafkaCheckpointLogKeySerde
  val grouperFactory = config.get(JobConfig.SSP_GROUPER_FACTORY)

  // producer is volatile for visibility since it is initialized in the main-thread, and accessed in other threads
  @volatile var systemProducer: SystemProducer = null
  var systemConsumer: SystemConsumer = null
  val systemAdmin: SystemAdmin = systemFactory.getAdmin(checkpointSystem, config)

  var taskNames = Set[TaskName]()
  var taskNamesToOffsets: Map[TaskName, Checkpoint] = null

  info("Creating KafkaCheckpointManager for checkpointTopic=%s, systemName=%s" format(checkpointTopic, checkpointSystem))

  /**
    * @inheritdoc
    */
  override def start {
    info("Creating checkpoint stream")
    systemAdmin.createStream(checkpointSpec)

    // register and start a producer for the checkpoint topic
    if (systemProducer == null) {
      info("Starting checkpoint SystemProducer")
      systemProducer = systemFactory.getProducer(checkpointSystem, config, metricsRegistry)
      systemProducer.register("checkpoint-source")
      systemProducer.start
    }

    // register and start a consumer for the checkpoint topic
    if (systemConsumer == null) {
      val oldestOffset = getOldestOffset(checkpointSsp)
      info(s"Starting checkpoint SystemConsumer from oldest offset $oldestOffset")
      systemConsumer = systemFactory.getConsumer(checkpointSystem, config, metricsRegistry)
      systemConsumer.register(checkpointSsp, oldestOffset)
      systemConsumer.start()
    }

    if (failOnCheckpointValidation) {
      info(s"Validating checkpoint stream")
      systemAdmin.validateStream(checkpointSpec)
    }
  }

  /**
    * @inheritdoc
    */
  override def register(taskName: TaskName) {
    debug(s"Registering taskName: $taskName ")
    taskNames += taskName
  }

  /**
    * @inheritdoc
    */
  override def readLastCheckpoint(taskName: TaskName): Checkpoint = {
    if (!taskNames.contains(taskName)) {
      throw new SamzaException(s"Task: $taskName is not registered with this CheckpointManager")
    }

    info(s"Reading checkpoint for taskName $taskName")

    if (taskNamesToOffsets == null) {
      info("Reading checkpoints for the first time")
      taskNamesToOffsets = readCheckpoints()
    } else {
      info("Updating existing checkpoint mappings")
      taskNamesToOffsets ++= readCheckpoints()
      info("Updating existing checkpoint mappings" + taskNamesToOffsets)
    }

    val checkpoint = taskNamesToOffsets.get(taskName).getOrElse(null)

    info(s"Got checkpoint state for taskName $taskName $checkpoint")
    checkpoint
  }

  /**
    * @inheritdoc
    */
  override def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint) {
    val key = new KafkaCheckpointLogKey(grouperFactory, taskName, KafkaCheckpointLogKey.CHECKPOINT_TYPE)
    val keyBytes = checkpointKeySerde.toBytes(key)
    val msgBytes = checkpointMsgSerde.toBytes(checkpoint)
    val envelope = new OutgoingMessageEnvelope(checkpointSsp, keyBytes, msgBytes)
    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy

    retryBackoff.run(
      loop => {
        systemProducer.send(taskName.getTaskName, envelope)
        systemProducer.flush(taskName.getTaskName) // make sure it is written
        debug("Completed writing checkpoint=%s into %s topic for system %s." format(checkpoint, checkpointTopic, checkpointSystem))
        loop.done
      },

      (exception, loop) => {
        warn(s"Failed to write checkpoint. key: $key", exception)
      }
    )
  }

  /**
    * @inheritdoc
    */
  override def clearCheckpoints = {
    info("Clear checkpoint stream %s in system %s" format(checkpointTopic, checkpointSystem))
    systemAdmin.clearStream(checkpointSpec)
  }

  override def stop = {
    if (systemProducer != null) {
      systemProducer.stop
      systemProducer = null
    }

    if (systemConsumer != null) {
      systemConsumer.stop
      systemConsumer = null
    }
    info("CheckpointManager stopped.")
  }

  /**
    * Returns the checkpoints from the log.
    *
    * <p> The underlying {@link SystemConsumer} is stateful and tracks its offsets. Hence, each invocation of this method
    * will read the log from where it left off previously. This allows for multiple efficient calls to [[readLastCheckpoint()]]
    */
  private def readCheckpoints(): Map[TaskName, Checkpoint] = {
    val checkpoints = mutable.Map[TaskName, Checkpoint]()

    val iterator = new SystemStreamPartitionIterator(systemConsumer, checkpointSsp)
    var numMessagesRead = 0

    while (iterator.hasNext) {
      numMessagesRead += 1
      val checkpointEnvelope: IncomingMessageEnvelope = iterator.next

      val keyBytes = checkpointEnvelope.getKey.asInstanceOf[Array[Byte]]
      if (keyBytes == null) {
        throw new SamzaException(s"Encountered a checkpoint message without key. Topic:$checkpointTopic " +
          s"Offset:${checkpointEnvelope.getOffset}")
      }

      val checkpointKey = checkpointKeySerde.fromBytes(keyBytes)
      val expectedGrouperFactory = checkpointKey.getGrouperFactoryClassName
      if (!grouperFactory.equals(expectedGrouperFactory)) {
        throw new SamzaException("SSPGrouperFactory in the checkpoint topic does not match the configured value" +
          s"Found: $grouperFactory Expected: $expectedGrouperFactory")
      }

      val checkpointBytes = checkpointEnvelope.getMessage.asInstanceOf[Array[Byte]]
      val checkpoint = checkpointMsgSerde.fromBytes(checkpointBytes)
      checkpoints.put(checkpointKey.getTaskName, checkpoint)
    }
    info(s"Read $numMessagesRead messages from system:$checkpointSystem topic:$checkpointTopic")
    checkpoints.toMap
  }

  /**
    * Returns the oldest available offset for the provided [[SystemStreamPartition]].
    */
  private def getOldestOffset(ssp: SystemStreamPartition): String = {
    val topic: String = ssp.getSystemStream.getStream
    val partition: Partition = ssp.getPartition

    val metaDataMap = systemAdmin.getSystemStreamMetadata(Collections.singleton(topic))
    val checkpointMetadata: SystemStreamMetadata = metaDataMap.get(topic)
    if (checkpointMetadata == null) {
      throw new SamzaException("Cannot get metadata for system=%s, topic=%s" format(checkpointSystem, topic))
    }

    val partitionMetaData = checkpointMetadata.getSystemStreamPartitionMetadata().get(partition)
    if (partitionMetaData == null) {
      throw new SamzaException("Cannot get partitionMetaData for system=%s, topic=%s" format(checkpointSystem, topic))
    }

    return partitionMetaData.getOldestOffset
  }
}
