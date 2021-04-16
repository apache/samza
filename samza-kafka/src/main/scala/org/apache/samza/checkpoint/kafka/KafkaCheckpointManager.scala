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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager, CheckpointV1, CheckpointV2}
import org.apache.samza.config.{Config, JobConfig, TaskConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.serializers.{CheckpointV1Serde, CheckpointV2Serde, Serde}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system._
import org.apache.samza.system.kafka.KafkaStreamSpec
import org.apache.samza.util.Logging
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
                             checkpointV1MsgSerde: Serde[CheckpointV1] = new CheckpointV1Serde,
                             checkpointV2MsgSerde: Serde[CheckpointV2] = new CheckpointV2Serde,
                             checkpointKeySerde: Serde[KafkaCheckpointLogKey] = new KafkaCheckpointLogKeySerde) extends CheckpointManager with Logging {

  var MaxRetryDurationInMillis: Long = TimeUnit.MINUTES.toMillis(15)

  val checkpointSystem: String = checkpointSpec.getSystemName
  val checkpointTopic: String = checkpointSpec.getPhysicalName

  info(s"Creating KafkaCheckpointManager for checkpointTopic:$checkpointTopic, systemName:$checkpointSystem " +
    s"validateCheckpoints:$validateCheckpoint")

  val checkpointSsp: SystemStreamPartition = new SystemStreamPartition(checkpointSystem, checkpointTopic, new Partition(0))
  val expectedGrouperFactory: String = new JobConfig(config).getSystemStreamPartitionGrouperFactory

  val systemConsumer = systemFactory.getConsumer(checkpointSystem, config, metricsRegistry, this.getClass.getSimpleName)
  val systemAdmin =  systemFactory.getAdmin(checkpointSystem, config, this.getClass.getSimpleName)

  var taskNames: Set[TaskName] = Set[TaskName]()
  var taskNamesToCheckpoints: Map[TaskName, Checkpoint] = _

  val producerRef: AtomicReference[SystemProducer] = new AtomicReference[SystemProducer](getSystemProducer())
  val producerCreationLock: Object = new Object

  // if true, systemConsumer can be safely closed after the first call to readLastCheckpoint.
  // if false, it must be left open until KafkaCheckpointManager::stop is called.
  // for active containers, this will be set to true, while false for standby containers.
  val stopConsumerAfterFirstRead: Boolean = new TaskConfig(config).getCheckpointManagerConsumerStopAfterFirstRead

  val checkpointReadVersion: Short = new TaskConfig(config).getCheckpointReadVersion

  /**
    * Create checkpoint stream prior to start.
    *
    */
  override def createResources(): Unit = {
    val createResourcesSystemAdmin =  systemFactory.getAdmin(checkpointSystem, config, this.getClass.getSimpleName + "createResource")
    Preconditions.checkNotNull(createResourcesSystemAdmin)
    createResourcesSystemAdmin.start()
    try {
      info(s"Creating checkpoint stream: ${checkpointSpec.getPhysicalName} with " +
        s"partition count: ${checkpointSpec.getPartitionCount}")
      createResourcesSystemAdmin.createStream(checkpointSpec)

      if (validateCheckpoint) {
        info(s"Validating checkpoint stream")
        createResourcesSystemAdmin.validateStream(checkpointSpec)
      }
    } finally {
      createResourcesSystemAdmin.stop()
    }
  }

  /**
    * @inheritdoc
    */
  override def start(): Unit = {
    // register and start a producer for the checkpoint topic
    info("Starting the checkpoint SystemProducer")
    producerRef.get().start()
    info("Starting the checkpoint SystemAdmin")
    systemAdmin.start()
    // register and start a consumer for the checkpoint topic
    val oldestOffset = getOldestOffset(checkpointSsp)
    info(s"Starting the checkpoint SystemConsumer from oldest offset $oldestOffset")
    systemConsumer.register(checkpointSsp, oldestOffset)
    systemConsumer.start()
  }

  /**
    * @inheritdoc
    */
  override def register(taskName: TaskName) {
    debug(s"Registering taskName: $taskName")
    producerRef.get().register(taskName.getTaskName)
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
      info("Reading checkpoints for the first time")
      taskNamesToCheckpoints = readCheckpoints()
      if (stopConsumerAfterFirstRead) {
        info("Stopping system consumer")
        systemConsumer.stop()
      }
    } else if (!stopConsumerAfterFirstRead) {
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
    val envelope = buildOutgoingMessageEnvelope(taskName, checkpoint)

    // Used for exponential backoff retries on failure in sending messages through producer.
    val startTimeInMillis: Long = System.currentTimeMillis()
    var sleepTimeInMillis: Long = 1000
    val maxSleepTimeInMillis: Long = 10000
    var producerException: Exception = null
    while ((System.currentTimeMillis() - startTimeInMillis) <= MaxRetryDurationInMillis) {
      val currentProducer = producerRef.get()
      try {
        currentProducer.send(taskName.getTaskName, envelope)
        currentProducer.flush(taskName.getTaskName) // make sure it is written
        debug(s"Wrote checkpoint: $checkpoint for task: $taskName")
        return
      } catch {
        case exception: Exception => {
          producerException = exception
          warn(s"Retrying failed write for checkpoint: $checkpoint for task: $taskName", exception)
          // TODO: Remove this producer recreation logic after SAMZA-1393.
          val newProducer: SystemProducer = getSystemProducer()
          producerCreationLock.synchronized {
            if (producerRef.compareAndSet(currentProducer, newProducer)) {
              info(s"Stopping the checkpoint SystemProducer")
              currentProducer.stop()
              info(s"Recreating the checkpoint SystemProducer")
              // SystemProducer contract is that clients call register(taskName) followed by start
              // before invoking writeCheckpoint, readCheckpoint API. Hence list of taskName are not
              // expected to change during the producer recreation.
              for (taskName <- taskNames) {
                debug(s"Registering the taskName: $taskName with SystemProducer")
                newProducer.register(taskName.getTaskName)
              }
              newProducer.start()
            } else {
              info("Producer instance was recreated by other thread. Retrying with it.")
              newProducer.stop()
            }
          }
        }
      }
      sleepTimeInMillis = Math.min(sleepTimeInMillis * 2, maxSleepTimeInMillis)
      Thread.sleep(sleepTimeInMillis)
    }
    throw new SamzaException(s"Exception when writing checkpoint: $checkpoint for task: $taskName.", producerException)
  }

  /**
    * @inheritdoc
    */
  override def clearCheckpoints(): Unit = {
    info("Clear checkpoint stream %s in system %s" format(checkpointTopic, checkpointSystem))
    systemAdmin.clearStream(checkpointSpec)
  }

  override def stop(): Unit = {
    info ("Stopping system admin.")
    systemAdmin.stop()

    info ("Stopping system producer.")
    producerRef.get().stop()

    if (!stopConsumerAfterFirstRead) {
      info("Stopping system consumer")
      systemConsumer.stop()
    }

    info("CheckpointManager stopped.")
  }

  @VisibleForTesting
  def getSystemProducer(): SystemProducer = {
    systemFactory.getProducer(checkpointSystem, config, metricsRegistry, this.getClass.getSimpleName)
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
          warn(s"Ignoring exception while deserializing checkpoint-key. Topic: $checkpointTopic Offset: $offset", e)
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

        val msgBytes = checkpointEnvelope.getMessage.asInstanceOf[Array[Byte]]
        try {
          // if checkpoint key version does not match configured checkpoint version to read, skip the message.
          if (checkpointReadVersion == CheckpointV1.CHECKPOINT_VERSION &&
            KafkaCheckpointLogKey.CHECKPOINT_V1_KEY_TYPE.equals(checkpointKey.getType)) {
            val msgBytes = checkpointEnvelope.getMessage.asInstanceOf[Array[Byte]]
            val checkpoint = checkpointV1MsgSerde.fromBytes(msgBytes)
            checkpoints.put(checkpointKey.getTaskName, checkpoint)
          } else if (checkpointReadVersion == CheckpointV2.CHECKPOINT_VERSION &&
            KafkaCheckpointLogKey.CHECKPOINT_V2_KEY_TYPE.equals(checkpointKey.getType)) {
            val checkpoint = checkpointV2MsgSerde.fromBytes(msgBytes)
            checkpoints.put(checkpointKey.getTaskName, checkpoint)
          } // else ignore and skip the message
        } catch {
          case e: Exception => throw new SamzaException(s"Exception while deserializing checkpoint-message. " +
            s"Topic: $checkpointTopic Offset: $offset", e)
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

    val partitionMetaData = checkpointMetadata.getSystemStreamPartitionMetadata.get(partition)
    if (partitionMetaData == null) {
      throw new SamzaException(s"Got a null partition metadata for system:$checkpointSystem, topic:$topic")
    }

    partitionMetaData.getOldestOffset
  }

  private def buildOutgoingMessageEnvelope[T <: Checkpoint](taskName: TaskName, checkpoint: T): OutgoingMessageEnvelope = {
    checkpoint match {
      case checkpointV1: CheckpointV1 => {
        val key = new KafkaCheckpointLogKey(
          KafkaCheckpointLogKey.CHECKPOINT_V1_KEY_TYPE, taskName, expectedGrouperFactory)
        val keyBytes = try {
          checkpointKeySerde.toBytes(key)
        } catch {
          case e: Exception =>
            throw new SamzaException(s"Exception when writing checkpoint-key for $taskName: $checkpoint", e)
        }
        val msgBytes = try {
          checkpointV1MsgSerde.toBytes(checkpointV1)
        } catch {
          case e: Exception =>
            throw new SamzaException(s"Exception when writing checkpoint for $taskName: $checkpoint", e)
        }
        new OutgoingMessageEnvelope(checkpointSsp, keyBytes, msgBytes)
      }
      case checkpointV2: CheckpointV2 => {
        val key = new KafkaCheckpointLogKey(
          KafkaCheckpointLogKey.CHECKPOINT_V2_KEY_TYPE, taskName, expectedGrouperFactory)
        val keyBytes = try {
          checkpointKeySerde.toBytes(key)
        } catch {
          case e: Exception =>
            throw new SamzaException(s"Exception when writing checkpoint-key for $taskName: $checkpoint", e)
        }
        val msgBytes = try {
          checkpointV2MsgSerde.toBytes(checkpointV2)
        } catch {
          case e: Exception =>
            throw new SamzaException(s"Exception when writing checkpoint for $taskName: $checkpoint", e)
        }
        new OutgoingMessageEnvelope(checkpointSsp, keyBytes, msgBytes)
      }
      case _ => throw new SamzaException("Unknown checkpoint version: " + checkpoint.getVersion)
    }
  }
}
