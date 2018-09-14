package org.apache.samza.system.kafka

import kafka.admin.AdminUtils
import kafka.utils.{Logging, ZkUtils}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.kafka.KafkaSystemAdmin.{CLEAR_STREAM_RETRIES, debug}
import org.apache.samza.system.{StreamSpec, SystemStreamMetadata, SystemStreamPartition}
import org.apache.samza.util.ExponentialSleepStrategy
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class KafkaSystemAdminUtils(systemName: String) {

  val LOG: Logger = LoggerFactory.getLogger("KafkaSystemAdminUtils")

  def createStream(kSpec: KafkaStreamSpec, connectZk: java.util.function.Supplier[ZkUtils]): Boolean = {
    LOG.info("Create topic %s in system %s" format(kSpec.getPhysicalName, systemName))
    var streamCreated = false

    new ExponentialSleepStrategy(initialDelayMs = 500).run(
      loop => {
        val zkClient = connectZk.get()
        try {
          AdminUtils.createTopic(
            zkClient,
            kSpec.getPhysicalName,
            kSpec.getPartitionCount,
            kSpec.getReplicationFactor,
            kSpec.getProperties)
        } finally {
          zkClient.close
        }

        streamCreated = true
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: TopicExistsException =>
            streamCreated = false
            loop.done
          case e: Exception =>
            LOG.warn("Failed to create topic %s: %s. Retrying." format(kSpec.getPhysicalName, e))
            LOG.debug("Exception detail:", e)
        }
      })

    streamCreated
  }


  /**
    * @inheritdoc
    *
    * Delete a stream in Kafka. Deleting topics works only when the broker is configured with "delete.topic.enable=true".
    * Otherwise it's a no-op.
    */
   def clearStream(spec: StreamSpec, connectZk: java.util.function.Supplier[ZkUtils]): Unit = {
    LOG.info("Delete topic %s in system %s" format (spec.getPhysicalName, systemName))
    val kSpec = KafkaStreamSpec.fromSpec(spec)
    var retries = CLEAR_STREAM_RETRIES
    new ExponentialSleepStrategy().run(
      loop => {
        val zkClient = connectZk.get()
        try {
          AdminUtils.deleteTopic(
            zkClient,
            kSpec.getPhysicalName)
        } finally {
          zkClient.close
        }

        loop.done
      },

      (exception, loop) => {
        if (retries > 0) {
          LOG.warn("Exception while trying to delete topic %s: %s. Retrying." format (spec.getPhysicalName, exception))
          retries -= 1
        } else {
          LOG.warn("Fail to delete topic %s: %s" format (spec.getPhysicalName, exception))
          loop.done
          throw exception
        }
      })
  }

}

object  KafkaSystemAdminUtils extends Logging {
  /**
    * A helper method that takes oldest, newest, and upcoming offsets for each
    * system stream partition, and creates a single map from stream name to
    * SystemStreamMetadata.
    */
  def assembleMetadata(oldestOffsets: Map[SystemStreamPartition, String], newestOffsets: Map[SystemStreamPartition, String], upcomingOffsets: Map[SystemStreamPartition, String]): Map[String, SystemStreamMetadata] = {
    val allMetadata = (oldestOffsets.keySet ++ newestOffsets.keySet ++ upcomingOffsets.keySet)
      .groupBy(_.getStream)
      .map {
        case (streamName, systemStreamPartitions) =>
          val streamPartitionMetadata = systemStreamPartitions
            .map(systemStreamPartition => {
              val partitionMetadata = new SystemStreamPartitionMetadata(
                // If the topic/partition is empty then oldest and newest will
                // be stripped of their offsets, so default to null.
                oldestOffsets.getOrElse(systemStreamPartition, null),
                newestOffsets.getOrElse(systemStreamPartition, null),
                upcomingOffsets(systemStreamPartition))
              (systemStreamPartition.getPartition, partitionMetadata)
            })
            .toMap
          val streamMetadata = new SystemStreamMetadata(streamName, streamPartitionMetadata.asJava)
          (streamName, streamMetadata)
      }
      .toMap

    // This is typically printed downstream and it can be spammy, so debug level here.
    debug("Got metadata: %s" format allMetadata)

    allMetadata
  }
}
