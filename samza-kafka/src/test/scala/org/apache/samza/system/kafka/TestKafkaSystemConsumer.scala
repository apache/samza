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