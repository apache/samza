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

package org.apache.samza.util

import java.util.Properties
import kafka.admin.AdminUtils
import kafka.api.{FetchRequestBuilder, PartitionOffsetRequestInfo, OffsetRequest, TopicMetadata}
import kafka.common.{ErrorMapping, TopicAndPartition, TopicExistsException}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.producer.{Producer, KeyedMessage}
import kafka.utils.{Utils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.apache.samza.checkpoint.kafka.{KafkaCheckpointManagerFactory, KafkaCheckpointLogKey, KafkaCheckpointException}
import org.apache.samza.config.Config
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManagerFactory}
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.{KafkaConfig, Config, StreamConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.{GroupByPartitionFactory, SystemStreamPartitionGrouperFactory}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.kafka.TopicMetadataCache
import org.apache.samza.system.{SystemStream, SystemFactory, SystemStreamPartition}
import org.apache.samza.{Partition, SamzaException}
import org.codehaus.jackson.map.ObjectMapper
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import java.util.regex.Pattern


/**
 * <p> Command-line tool for migrating checkpoint topic for a job from 0.7 format to 0.8 format. </p>
 *
 * <p> When running this tool, you need to provide the configuration URI of job you
 * want to migrate. </p>
 *
 * <p> If you're building Samza from source, you can use the 'checkpointMigrationTool' gradle
 * task as a short-cut to running this tool.</p>
 */
object CheckpointMigrationTool {
  val DEFAULT_SSP_GROUPER_FACTORY = classOf[GroupByPartitionFactory].getCanonicalName

  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val tool = new CheckpointMigrationTool(config)
    tool.run
  }
}

class CheckpointMigrationTool(config: Config) extends Logging {

  def run {
    val kafkaHelper = new KafkaHelper(config)
    val newCheckpointTopic = kafkaHelper.getNewCheckpointTopic
    val oldCheckpointTopic = kafkaHelper.getOldCheckpointTopic
    val newCheckpointTopicExists = kafkaHelper.checkTopicExists(newCheckpointTopic)
    val oldCheckpointTopicExists = kafkaHelper.checkTopicExists(oldCheckpointTopic)

    if(!oldCheckpointTopicExists) {
      info("Checkpoint Topic in old format - %s does not exist. Nothing to do here. Quitting!" format oldCheckpointTopic)
    } else {
      if(newCheckpointTopicExists) {
        info("Checkpoint Topic in new format - %s already exists!" format newCheckpointTopic)
        info("Proceeding may overwrite more recent offsets in the checkpoint topic.")
        info("Do you still wish to continue (yes/no)? ")
        val answer = Console.readLine()
        if(answer.toLowerCase.equals("yes")) {
          writeNewCheckpoints(kafkaHelper, oldCheckpointTopic, newCheckpointTopic, false)
        }
      } else {
        writeNewCheckpoints(kafkaHelper, oldCheckpointTopic, newCheckpointTopic, true)
      }
    }
  }

  private def writeNewCheckpoints(kafkaHelper: KafkaHelper, oldCheckpointTopic: String, newCheckpointTopic: String, createTopic: Boolean) = {
    //Read most recent offsets from 0.7 checkpoint topic
    val ssp = Util.getInputStreamPartitions(config)
    val partitions = ssp.map(_.getPartition).toSet
    val lastCheckpoint = partitions.flatMap(
      partition => {
        val cp = kafkaHelper.readLastCheckpoint(oldCheckpointTopic, partition)
        if(cp != null) {
          cp.getOffsets.map {
            case (systemStream, offset) => new SystemStreamPartition(systemStream, partition) -> offset
          }
        } else {
          throw new KafkaCheckpointException("Received null as offset for checkpoint topic %s partition %d" format(oldCheckpointTopic, partition.getPartitionId))
        }
      }).toMap

    //Group input streams by partition (Default for 0.7 implementations)
    val factory = Util.getObj[SystemStreamPartitionGrouperFactory](CheckpointMigrationTool.DEFAULT_SSP_GROUPER_FACTORY)
    val taskNameToSSP = factory.getSystemStreamPartitionGrouper(config).group(lastCheckpoint.keySet)

    //Create topic, if it doesn't already exist
    if(createTopic) {
      kafkaHelper.createTopic(newCheckpointTopic)
    }
    taskNameToSSP.map {
      entry =>
        val taskName = entry._1
        val ssps = entry._2
        val checkpoints = ssps.map {
          ssp =>
            ssp -> lastCheckpoint
                    .get(ssp)
                    .getOrElse(
                      new KafkaCheckpointException("Did not find checkpoint for %s partition %d" format(ssp.getSystemStream.toString, ssp.getPartition.getPartitionId)))
                    .toString
        }.toMap
        kafkaHelper.writeCheckpoint(taskName, new Checkpoint(checkpoints), newCheckpointTopic)
    }
  }
}

class KafkaHelper(config: Config) extends Logging {
  val CHECKPOINT_LOG_VERSION_NUMBER = 1
  val INJECTED_PRODUCER_PROPERTIES = Map("request.required.acks" -> "-1",
                                         "compression.codec" -> "none",
                                         "producer.type" -> "sync",
                                         "message.send.max.retries" -> (Integer.MAX_VALUE - 1).toString)

  private val getCheckpointTopicProperties = {
    val segmentBytes = config.getCheckpointSegmentBytes.getOrElse("26214400")
    (new Properties /: Map("cleanup.policy" -> "compact", "segment.bytes" -> segmentBytes)) {
      case (props, (k, v)) => props.put(k, v); props
    }
  }

  val retryBackoff = new ExponentialSleepStrategy
  val jsonMapper = new ObjectMapper()

  //Parse config data
  val jobName = config.getName.getOrElse(throw new SamzaException("Missing job name in configs"))
  val jobId = config.getJobId.getOrElse("1")
  val clientId = KafkaUtil.getClientId("samza-checkpoint-migration-tool", config)
  val replicationFactor = config.getCheckpointReplicationFactor.getOrElse("3").toInt

  val systemName = config.getCheckpointSystem.getOrElse(
    throw new SamzaException("no system defined for Kafka's checkpoint manager."))

  //Kafka Producer Config
  val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId, INJECTED_PRODUCER_PROPERTIES)
  val brokersListString = Option(producerConfig.brokerList).getOrElse(throw new SamzaException(
    "No broker list defined in config for %s." format systemName))

  private val connectProducer = () => { new Producer[Array[Byte], Array[Byte]](producerConfig)  }

  //Kafka Consumer Config
  val consumerConfig = config.getKafkaSystemConsumerConfig(systemName, clientId)
  val metadataStore = new ClientUtilTopicMetadataStore(brokersListString, clientId, consumerConfig.socketTimeoutMs)
  val zkConnect = Option(consumerConfig.zkConnect).getOrElse(throw new SamzaException(
    "no zookeeper.connect defined in config"))

  private val connectZk = () => { new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer) }

  def getOldCheckpointTopic: String = {
    "__samza_checkpoint_%s_%s" format(jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
  }

  def getNewCheckpointTopic: String = KafkaCheckpointManagerFactory.getTopic(jobName, jobId)

  private def getTopicMetadata(topicName: String): TopicMetadata = {
    val metadataMap = TopicMetadataCache.getTopicMetadata(Set(topicName),
                                                          systemName,
                                                          (topics: Set[String]) => metadataStore.getTopicInfo(topics))
    metadataMap(topicName)
  }

  private def getKafkaConsumer(metadata: TopicMetadata, partition: Partition) = {
    val partitionMetadata = metadata.partitionsMetadata
            .filter(_.partitionId == partition.getPartitionId)
            .headOption
            .getOrElse(throw new KafkaCheckpointException("Tried to find partition information for partition %d, but it didn't exist in Kafka." format partition.getPartitionId))
    val leader = partitionMetadata.leader.getOrElse(throw new SamzaException("No leader available for topic %s" format metadata.topic))

    info("Connecting to leader %s:%d for topic %s and partition %s to fetch last checkpoint message." format(leader.host, leader.port, metadata.topic, partition.getPartitionId))
    new SimpleConsumer(leader.host,
                       leader.port,
                       consumerConfig.socketTimeoutMs,
                       consumerConfig.socketReceiveBufferBytes,
                       clientId)
  }

  def checkTopicExists(topicName: String): Boolean = {
    val zkClient = connectZk()
    try {
      AdminUtils.topicExists(zkClient = zkClient, topic = topicName)
    } catch {
      case e: Exception =>
        warn("Failed to check for existence of topic %s : %s" format(topicName, e))
        debug("Exception detail:", e)
        false
    } finally {
      zkClient.close
    }
  }

  def createTopic(topicName: String) = {
    info("Attempting to create checkpoint topic %s." format topicName)
    retryBackoff.run(
      loop => {
        val zkClient = connectZk()
        try {
          AdminUtils.createTopic(zkClient, topicName, 1, replicationFactor, getCheckpointTopicProperties)
        } finally {
          zkClient.close
        }
        info("Created checkpoint topic %s." format topicName)
        loop.done
      },
      (exception, loop) => {
        exception match {
          case e: TopicExistsException =>
            info("Checkpoint topic %s already exists." format topicName)
            loop.done
          case e: Exception =>
            warn("Failed to create topic %s: %s. Retrying." format(topicName, e))
            debug("Exception detail:", e)
        }
      })
  }

  def readLastCheckpoint(topicName: String, partition: Partition): Checkpoint = {
    info("Reading checkpoint for partition %s." format partition.getPartitionId)
    val metadata = getTopicMetadata(topicName)
    val checkpoint = retryBackoff.run(
      loop => {
        val consumer = getKafkaConsumer(metadata, partition)
        val partitionId = partition.getPartitionId
        try {
          val topicAndPartition = new TopicAndPartition(topicName, partitionId)
          val offsetResponse = consumer.getOffsetsBefore(new OffsetRequest(
            Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))))
                  .partitionErrorAndOffsets.get(topicAndPartition)
                  .getOrElse(throw new KafkaCheckpointException("Unable to find offset information for %s:%d" format(topicName, partitionId)))

          // Fail or retry if there was an an issue with the offset request.
          ErrorMapping.maybeThrowException(offsetResponse.error)

          val offset = offsetResponse.offsets.headOption.getOrElse(
            throw new KafkaCheckpointException("Got response, but no offsets defined for %s:%d" format(topicName, partitionId)))

          info("Got offset %s for topic %s and partition %s. Attempting to fetch message." format(offset, topicName, partitionId))

          if (offset <= 0) {
            info("Got offset 0 (no messages in checkpoint topic) for topic %s and partition %s, so returning null. If you expected the checkpoint topic to have messages, you're probably going to lose data." format(topicName, partition))
            return null
          }

          val request = new FetchRequestBuilder()
                  .addFetch(topicName,
                            partitionId,
                            offset - 1,
                            consumerConfig.fetchMessageMaxBytes).maxWait(500).minBytes(1).clientId(clientId).build
          val messageSet = consumer.fetch(request)
          if (messageSet.hasError) {
            warn("Got error code from broker for %s: %s" format(topicName, messageSet.errorCode(topicName,
                                                                                                partitionId)))
            val errorCode = messageSet.errorCode(topicName, partitionId)
            if (ErrorMapping.OffsetOutOfRangeCode.equals(errorCode)) {
              warn("Got an offset out of range exception while getting last checkpoint for topic %s and partition %s, so returning a null offset to the KafkaConsumer. Let it decide what to do based on its autooffset.reset setting." format(topicName, partitionId))
              return null
            }
            ErrorMapping.maybeThrowException(errorCode)
          }
          val messages = messageSet.messageSet(topicName, partitionId).toList

          if (messages.length != 1) {
            throw new KafkaCheckpointException("Something really unexpected happened. Got %s "
                                                       + "messages back when fetching from checkpoint topic %s and partition %s. "
                                                       + "Expected one message. It would be unsafe to go on without the latest checkpoint, "
                                                       + "so failing." format(messages.length, topicName, partition))
          }

          val checkpoint = jsonMapper.readValue(Utils.readBytes(messages(0).message.payload),
                                                classOf[java.util.Map[String, java.util.Map[String, String]]]).flatMap {
            case (systemName, streamToOffsetMap) =>
              streamToOffsetMap.map {
                case (streamName, offset) => (new SystemStreamPartition(systemName, streamName, partition), offset)
              }
          }
          val results = JavaConversions.mapAsJavaMap(checkpoint)
          loop.done
          return new Checkpoint(results)
        } finally {
          consumer.close
        }
      },
      (exception, loop) => {
        exception match {
          case e: KafkaCheckpointException => throw e
          case e: Exception =>
            warn("While trying to read last checkpoint for topic %s and partition %s: %s. Retrying." format(topicName, partition, e))
            debug("Exception detail:", e)
        }
      }).getOrElse(throw new SamzaException("Failed to get checkpoint for partition %s" format partition.getPartitionId))

    info("Got checkpoint state for partition %s: %s" format(partition.getPartitionId, checkpoint))
    checkpoint
  }

  def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint, topicName: String) = {
    KafkaCheckpointLogKey.setSystemStreamPartitionGrouperFactoryString(classOf[GroupByPartitionFactory].getCanonicalName)
    val serde = new CheckpointSerde
    val key = KafkaCheckpointLogKey.getCheckpointKey(taskName)
    val keyBytes = key.toBytes()
    val msgBytes = serde.toBytes(checkpoint)

    var producer = connectProducer()
    retryBackoff.run(
      loop => {
        if (producer == null) {
          producer = connectProducer()
        }

        producer.send(new KeyedMessage(topicName, keyBytes, 0, msgBytes))
        loop.done
      },
      (exception, loop) => {
        warn("Failed to write partition entry %s: %s. Retrying." format(key, exception))
        debug("Exception detail:", exception)
        if (producer != null) {
          producer.close
        }
        producer = null
      })
  }
}
