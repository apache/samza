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

import com.google.common.base.Preconditions
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.config.{Config, JobConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.serializers.Serde
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system._
import org.apache.samza.system.kafka.KafkaStreamSpec
import org.apache.samza.util.{ExponentialSleepStrategy, Logging}
import org.apache.samza.{Partition, SamzaException}

import scala.collection.mutable

/**
  * A [[CheckpointManager]] that uses a compacted Kafka topic-partition to store the [[Checkpoint]] corresponding to
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
                             validateCheckpoint: Boolean,
                             config: Config,
                             metricsRegistry: MetricsRegistry,
                             checkpointMsgSerde: Serde[Checkpoint] = new CheckpointSerde,
                             checkpointKeySerde: Serde[KafkaCheckpointLogKey] = new KafkaCheckpointLogKeySerde) extends CheckpointManager with Logging {

  info(s"Creating KafkaCheckpointManager for checkpointTopic:$checkpointTopic, systemName:$checkpointSystem " +
    s"validateCheckpoints:$validateCheckpoint")

  val checkpointSystem: String = checkpointSpec.getSystemName
  val checkpointTopic: String = checkpointSpec.getPhysicalName
  val checkpointSsp = new SystemStreamPartition(checkpointSystem, checkpointTopic, new Partition(0))
  val expectedGrouperFactory = new JobConfig(config).getSystemStreamPartitionGrouperFactory

  val systemProducer = systemFactory.getProducer(checkpointSystem, config, metricsRegistry)
  val systemConsumer = systemFactory.getConsumer(checkpointSystem, config, metricsRegistry)
  val systemAdmin = systemFactory.getAdmin(checkpointSystem, config)

  var taskNames = Set[TaskName]()
  var taskNamesToCheckpoints: Map[TaskName, Checkpoint] = null


  /**
    * @inheritdoc
    */
  override def start {
    Preconditions.checkNotNull(systemProducer)
    Preconditions.checkNotNull(systemConsumer)
    Preconditions.checkNotNull(systemAdmin)

    info(s"Creating checkpoint stream: ${checkpointSpec.getPhysicalName} with " +
      s"partition count: ${checkpointSpec.getPartitionCount}")
    systemAdmin.createStream(checkpointSpec)

    // register and start a producer for the checkpoint topic
    systemProducer.start

    // register and start a consumer for the checkpoint topic
    val oldestOffset = getOldestOffset(checkpointSsp)
    info(s"Starting checkpoint SystemConsumer from oldest offset $oldestOffset")
    systemConsumer.register(checkpointSsp, oldestOffset)
    systemConsumer.start

    if (validateCheckpoint) {
      info(s"Validating checkpoint stream")
      systemAdmin.validateStream(checkpointSpec)
    }
  }

  /**
    * @inheritdoc
    */
  override def register(taskName: TaskName) {
    debug(s"Registering taskName: $taskName")
    systemProducer.register(taskName.getTaskName)
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

    if (taskNamesToCheckpoints == null) {
      debug("Reading checkpoints for the first time")
      taskNamesToCheckpoints = readCheckpoints()
    } else {
      debug("Updating existing checkpoint mappings")
      taskNamesToCheckpoints ++= readCheckpoints()
    }

    val checkpoint: Checkpoint = taskNamesToCheckpoints.getOrElse(taskName, null)

    info(s"Got checkpoint state for taskName - $taskName: $checkpoint")
    checkpoint
  }

  /**
    * @inheritdoc
    */
  override def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint) {
    val key = new KafkaCheckpointLogKey(KafkaCheckpointLogKey.CHECKPOINT_KEY_TYPE, taskName, expectedGrouperFactory)
    val keyBytes = try {
      checkpointKeySerde.toBytes(key)
    } catch {
      case e: Exception => throw new SamzaException(s"Exception when writing checkpoint-key for $taskName: $checkpoint", e)
    }
    val msgBytes = try {
      checkpointMsgSerde.toBytes(checkpoint)
    } catch {
      case e: Exception => throw new SamzaException(s"Exception when writing checkpoint for $taskName: $checkpoint", e)
    }

    val envelope = new OutgoingMessageEnvelope(checkpointSsp, keyBytes, msgBytes)
    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy

    retryBackoff.run(
      loop => {
        systemProducer.send(taskName.getTaskName, envelope)
        systemProducer.flush(taskName.getTaskName) // make sure it is written
        debug(s"Wrote checkpoint: $checkpoint for task: $taskName")
        loop.done
      },

      (exception, loop) => {
        warn(s"Retrying failed checkpoint write to key: $key, checkpoint: $checkpoint for task: $taskName", exception)
      }
    )
  }

  /**
    * @inheritdoc
    */
  override def clearCheckpoints: Unit = {
    info("Clear checkpoint stream %s in system %s" format(checkpointTopic, checkpointSystem))
    systemAdmin.clearStream(checkpointSpec)
  }

  override def stop = {
    if (systemProducer != null) {
      systemProducer.stop
    } else {
      error("Checkpoint SystemProducer should not be null")
    }

    if (systemConsumer != null) {
      systemConsumer.stop
    } else {
      error("Checkpoint SystemConsumer should not be null")
    }
    info("CheckpointManager stopped.")
  }

  /**
    * Returns the checkpoints from the log.
    *
    * <p> The underlying [[SystemConsumer]] is stateful and tracks its offsets. Hence, each invocation of this method
    * will read the log from where it left off previously. This allows for multiple efficient calls to [[readLastCheckpoint()]]
    */
  private def readCheckpoints(): Map[TaskName, Checkpoint] = {
    val checkpoints = mutable.Map[TaskName, Checkpoint]()

    val iterator = new SystemStreamPartitionIterator(systemConsumer, checkpointSsp)
    var numMessagesRead = 0

    while (iterator.hasNext) {
      val checkpointEnvelope: IncomingMessageEnvelope = iterator.next
      val offset = checkpointEnvelope.getOffset

      numMessagesRead += 1
      if (numMessagesRead % 1000 == 0) {
        info(s"Read $numMessagesRead from topic: $checkpointTopic. Current offset: $offset")
      }

      val keyBytes = checkpointEnvelope.getKey.asInstanceOf[Array[Byte]]
      if (keyBytes == null) {
        throw new SamzaException("Encountered a checkpoint message with null key. Topic:$checkpointTopic " +
          s"Offset:$offset")
      }

      val checkpointKey = try {
        checkpointKeySerde.fromBytes(keyBytes)
      } catch {
        case e: Exception => if (validateCheckpoint) {
          throw new SamzaException(s"Exception while serializing checkpoint-key. " +
            s"Topic: $checkpointTopic Offset: $offset", e)
        } else {
          warn(s"Ignoring exception while serializing checkpoint-key. Topic: $checkpointTopic Offset: $offset", e)
          null
        }
      }

      if (checkpointKey != null) {
        // If the grouper in the key is not equal to the configured grouper, error out.
        val actualGrouperFactory = checkpointKey.getGrouperFactoryClassName
        if (!expectedGrouperFactory.equals(actualGrouperFactory)) {
          warn(s"Grouper mismatch. Configured: $expectedGrouperFactory Actual: $actualGrouperFactory ")
          if (validateCheckpoint) {
            throw new SamzaException("SSPGrouperFactory in the checkpoint topic does not match the configured value" +
              s"Configured value: $expectedGrouperFactory; Actual value: $actualGrouperFactory Offset: $offset")
          }
        }

        // If the type of the key is not KafkaCheckpointLogKey.CHECKPOINT_KEY_TYPE, it can safely be ignored.
        if (KafkaCheckpointLogKey.CHECKPOINT_KEY_TYPE.equals(checkpointKey.getType)) {
          val checkpointBytes = checkpointEnvelope.getMessage.asInstanceOf[Array[Byte]]
          val checkpoint = try {
            checkpointMsgSerde.fromBytes(checkpointBytes)
          } catch {
            case e: Exception => throw new SamzaException(s"Exception while serializing checkpoint-message. " +
              s"Topic: $checkpointTopic Offset: $offset", e)
          }

          checkpoints.put(checkpointKey.getTaskName, checkpoint)
        }
      }
    }
    info(s"Read $numMessagesRead messages from system:$checkpointSystem topic:$checkpointTopic")
    checkpoints.toMap
  }

  /**
    * Returns the oldest available offset for the provided [[SystemStreamPartition]].
    */
  private def getOldestOffset(ssp: SystemStreamPartition): String = {
    val topic = ssp.getSystemStream.getStream
    val partition = ssp.getPartition

    val metaDataMap = systemAdmin.getSystemStreamMetadata(Collections.singleton(topic))
    val checkpointMetadata: SystemStreamMetadata = metaDataMap.get(topic)
    if (checkpointMetadata == null) {
      throw new SamzaException(s"Got null metadata for system:$checkpointSystem, topic:$topic")
    }

    val partitionMetaData = checkpointMetadata.getSystemStreamPartitionMetadata().get(partition)
    if (partitionMetaData == null) {
      throw new SamzaException(s"Got a null partition metadata for system:$checkpointSystem, topic:$topic")
    }

    return partitionMetaData.getOldestOffset
  }
}
