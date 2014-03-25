package org.apache.samza.system.kafka

import org.junit.Test
import org.junit.Assert._
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition
import kafka.common.TopicAndPartition

class TestKafkaSystemConsumer {
  @Test
  def testFetchThresholdShouldDivideEvenlyAmongPartitions {
    val consumer = new KafkaSystemConsumer("", "", new KafkaSystemConsumerMetrics, fetchThreshold = 50000) {
      override def refreshBrokers(topicPartitionsAndOffsets: Map[TopicAndPartition, String]) {
      }
    }

    for (i <- 0 until 50) {
      consumer.register(new SystemStreamPartition("test-system", "test-stream", new Partition(i)), "0")
    }

    consumer.start

    assertEquals(1000, consumer.perPartitionFetchThreshold)
  }
}