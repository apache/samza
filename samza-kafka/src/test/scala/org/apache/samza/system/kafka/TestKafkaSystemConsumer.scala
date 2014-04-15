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
import org.apache.samza.util.TopicMetadataStore
import kafka.api.TopicMetadata
import kafka.api.PartitionMetadata
import kafka.cluster.Broker

class TestKafkaSystemConsumer {
  @Test
  def testFetchThresholdShouldDivideEvenlyAmongPartitions {
    val metadataStore = new MockMetadataStore
    val consumer = new KafkaSystemConsumer("", "", new KafkaSystemConsumerMetrics, metadataStore, fetchThreshold = 50000) {
      override def refreshBrokers(topicPartitionsAndOffsets: Map[TopicAndPartition, String]) {
      }
    }

    for (i <- 0 until 50) {
      consumer.register(new SystemStreamPartition("test-system", "test-stream", new Partition(i)), "0")
    }

    consumer.start

    assertEquals(1000, consumer.perPartitionFetchThreshold)
  }

  @Test
  def testBrokerCreationShouldTriggerStart {
    val systemName = "test-system"
    val streamName = "test-stream"
    val metrics = new KafkaSystemConsumerMetrics
    // Lie and tell the store that the partition metadata is empty. We can't 
    // use partition metadata because it has Broker in its constructor, which 
    // is package private to Kafka. 
    val metadataStore = new MockMetadataStore(Map(streamName -> TopicMetadata(streamName, Seq.empty, 0)))
    var hosts = List[String]()
    var getHostPortCount = 0
    val consumer = new KafkaSystemConsumer(systemName, streamName, metrics, metadataStore) {
      override def getHostPort(topicMetadata: TopicMetadata, partition: Int): Option[(String, Int)] = {
        // Generate a unique host every time getHostPort is called.
        getHostPortCount += 1
        Some("localhost-%s" format getHostPortCount, 0)
      }

      override def createBrokerProxy(host: String, port: Int): BrokerProxy = {
        new BrokerProxy(host, port, systemName, "", metrics, sink) {
          override def addTopicPartition(tp: TopicAndPartition, nextOffset: Option[String]) = {
            // Skip this since we normally do verification of offsets, which 
            // tries to connect to Kafka. Rather than mock that, just forget it.
            nextOffsets.size
          }

          override def start {
            hosts :+= host
          }
        }
      }
    }

    consumer.register(new SystemStreamPartition(systemName, streamName, new Partition(0)), "1")
    assertEquals(0, hosts.size)
    consumer.start
    assertEquals(List("localhost-1"), hosts)
    // Should trigger a refresh with a new host.
    consumer.sink.abdicate(new TopicAndPartition(streamName, 0), 2)
    assertEquals(List("localhost-1", "localhost-2"), hosts)
  }
}

class MockMetadataStore(var metadata: Map[String, TopicMetadata] = Map()) extends TopicMetadataStore {
  def getTopicInfo(topics: Set[String]): Map[String, TopicMetadata] = metadata
}