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

import org.I0Itec.zkclient.ZkClient
import org.junit.Assert._
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.utils.TestZKUtils
import kafka.utils.Utils
import kafka.zk.EmbeddedZookeeper
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.Partition
import scala.collection._
import scala.collection.JavaConversions._
import org.apache.samza.util.{ ClientUtilTopicMetadataStore, TopicMetadataStore }
import org.apache.samza.config.MapConfig
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.system.SystemStream
import kafka.utils.ZKStringSerializer

object TestKafkaCheckpointManager {
  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  val brokerId1 = 0
  val brokerId2 = 1
  val brokerId3 = 2
  val ports = TestUtils.choosePorts(3)
  val (port1, port2, port3) = (ports(0), ports(1), ports(2))

  val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
  val config1 = new KafkaConfig(props1) {
    override val hostName = "localhost"
    override val numPartitions = 1
  }
  val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  val config2 = new KafkaConfig(props2) {
    override val hostName = "localhost"
    override val numPartitions = 1
  }
  val props3 = TestUtils.createBrokerConfig(brokerId3, port3)
  val config3 = new KafkaConfig(props3) {
    override val hostName = "localhost"
    override val numPartitions = 1
  }

  val config = new java.util.Properties()
  val brokers = "localhost:%d,localhost:%d,localhost:%d" format (port1, port2, port3)
  config.put("metadata.broker.list", brokers)
  config.put("producer.type", "sync")
  config.put("request.required.acks", "-1")
  val producerConfig = new ProducerConfig(config)
  val partition = new Partition(0)
  val cp1 = new Checkpoint(Map(new SystemStream("kafka", "topic") -> "123"))
  val cp2 = new Checkpoint(Map(new SystemStream("kafka", "topic") -> "12345"))
  var zookeeper: EmbeddedZookeeper = null
  var server1: KafkaServer = null
  var server2: KafkaServer = null
  var server3: KafkaServer = null
  var metadataStore: TopicMetadataStore = null

  @BeforeClass
  def beforeSetupServers {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    server1 = TestUtils.createServer(config1)
    server2 = TestUtils.createServer(config2)
    server3 = TestUtils.createServer(config3)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
  }

  @AfterClass
  def afterCleanLogDirs {
    server1.shutdown
    server1.awaitShutdown()
    server2.shutdown
    server2.awaitShutdown()
    server3.shutdown
    server3.awaitShutdown()
    Utils.rm(server1.config.logDirs)
    Utils.rm(server2.config.logDirs)
    Utils.rm(server3.config.logDirs)
  }
}

class TestKafkaCheckpointManager {
  import TestKafkaCheckpointManager._

  @Test
  def testCheckpointShouldBeNullIfStateTopicDoesNotExistShouldBeCreatedOnWriteAndShouldBeReadableAfterWrite {
    val kcm = getKafkaCheckpointManager
    kcm.register(partition)
    kcm.start
    var readCp = kcm.readLastCheckpoint(partition)
    // read before topic exists should result in a null checkpoint
    assert(readCp == null)
    // create topic the first time around
    kcm.writeCheckpoint(partition, cp1)
    readCp = kcm.readLastCheckpoint(partition)
    assert(cp1.equals(readCp))
    // should get an exception if partition doesn't exist
    try {
      readCp = kcm.readLastCheckpoint(new Partition(1))
      fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    } catch {
      case e: Exception => None // expected
    }
    // writing a second message should work, too
    kcm.writeCheckpoint(partition, cp2)
    readCp = kcm.readLastCheckpoint(partition)
    assert(cp2.equals(readCp))
    kcm.stop
  }

  private def getKafkaCheckpointManager = new KafkaCheckpointManager(
    clientId = "some-client-id",
    stateTopic = "state-topic",
    systemName = "kafka",
    totalPartitions = 1,
    replicationFactor = 3,
    socketTimeout = 30000,
    bufferSize = 64 * 1024,
    fetchSize = 300 * 1024,
    metadataStore = metadataStore,
    connectProducer = () => new Producer[Partition, Array[Byte]](producerConfig),
    connectZk = () => new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer))
}
