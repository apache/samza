package org.apache.samza.system.kafka

import org.junit.Test
import org.junit.Assert._
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition

class TestKafkaSystemConsumer {
  @Test
  def testFetchThresholdShouldDivideEvenlyAmongPartitions {
    val consumer = new KafkaSystemConsumer("", "", new KafkaSystemConsumerMetrics, fetchThreshold = 50000)

    for (i <- 0 until 50) {
      consumer.register(new SystemStreamPartition("test-system", "test-stream", new Partition(i)), "0")
    }

    assertEquals(1000, consumer.perPartitionFetchThreshold)
  }
}