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

package org.apache.samza.config

import java.net.URI
import java.io.File
import java.util.Properties

import kafka.consumer.ConsumerConfig

import org.apache.samza.config.factories.PropertiesConfigFactory
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class TestKafkaConfig {

  @Test
  def testIdGeneration = {
    val factory = new PropertiesConfigFactory()
    val props = new Properties
    props.setProperty(" systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory")
    props.setProperty("systems.kafka.consumer.zookeeper.connect", "localhost:2181/")
    props.setProperty("systems.kafka.producer.bootstrap.servers", "localhost:9092")

    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)

    val consumerConfig1 = kafkaConfig.getKafkaSystemConsumerConfig("kafka")
    val consumerClientId1 = consumerConfig1.clientId
    val groupId1 = consumerConfig1.groupId
    val consumerConfig2 = kafkaConfig.getKafkaSystemConsumerConfig("kafka")
    val consumerClientId2 = consumerConfig2.clientId
    val groupId2 = consumerConfig2.groupId
    assert(consumerClientId1.startsWith("undefined-samza-consumer-"))
    assert(consumerClientId2.startsWith("undefined-samza-consumer-"))
    assert(groupId1.startsWith("undefined-samza-consumer-group-"))
    assert(groupId2.startsWith("undefined-samza-consumer-group-"))
    assert(consumerClientId1 != consumerClientId2)
    assert(groupId1 != groupId2)

    val consumerConfig3 = kafkaConfig.getKafkaSystemConsumerConfig("kafka", "TestClientId", "TestGroupId")
    val consumerClientId3 = consumerConfig3.clientId
    val groupId3 = consumerConfig3.groupId
    assert(consumerClientId3 == "TestClientId")
    assert(groupId3 == "TestGroupId")

    val producerConfig1 = kafkaConfig.getKafkaSystemProducerConfig("kafka")
    val producerClientId1 = producerConfig1.clientId
    val producerConfig2 = kafkaConfig.getKafkaSystemProducerConfig("kafka")
    val producerClientId2 = producerConfig2.clientId

    assert(producerClientId1.startsWith("undefined-samza-producer-"))
    assert(producerClientId2.startsWith("undefined-samza-producer-"))
    assert(producerClientId1 != producerClientId2)

    val producerConfig3 = kafkaConfig.getKafkaSystemProducerConfig("kafka", "TestClientId")
    val producerClientId3 = producerConfig3.clientId
    assert(producerClientId3 == "TestClientId")

  }

  @Test
  def testStreamLevelFetchSizeOverride() {
    val props = new Properties
    props.setProperty("systems.kafka.consumer.zookeeper.connect", "localhost:2181/")
    props.setProperty("systems.kafka.producer.bootstrap.servers", "localhost:9092")

    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    val consumerConfig = kafkaConfig.getKafkaSystemConsumerConfig("kafka")
    // default fetch size
    assertEquals(1024*1024, consumerConfig.fetchMessageMaxBytes)

    props.setProperty("systems.kafka.consumer.fetch.message.max.bytes", "262144")
    val mapConfig1 = new MapConfig(props.toMap[String, String])
    val kafkaConfig1 = new KafkaConfig(mapConfig1)
    val consumerConfig1 = kafkaConfig1.getKafkaSystemConsumerConfig("kafka")
    // shared fetch size
    assertEquals(512*512, consumerConfig1.fetchMessageMaxBytes)
    
    props.setProperty("systems.kafka.streams.topic1.consumer.fetch.message.max.bytes", "65536")
    val mapConfig2 = new MapConfig(props.toMap[String, String])
    val kafkaConfig2 = new KafkaConfig(mapConfig2)
    val consumerConfig2 = kafkaConfig2.getFetchMessageMaxBytesTopics("kafka")
    // topic fetch size
    assertEquals(256*256, consumerConfig2 getOrElse ("topic1", 1024*1024))
  }

  @Test
  def testChangeLogProperties() {
    val props = (new Properties /: Map(
      "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
      "stores.test1.changelog" -> "kafka.mychangelog1",
      "stores.test2.changelog" -> "kafka.mychangelog2",
      "stores.test1.changelog.kafka.cleanup.policy" -> "delete"
      )) { case (props, (k, v)) => props.put(k, v); props }

    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test1").getProperty("cleanup.policy"), "delete")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test2").getProperty("cleanup.policy"), "compact")
    val storeToChangelog = kafkaConfig.getKafkaChangelogEnabledStores()
    assertEquals(storeToChangelog.get("test1").getOrElse(""), "mychangelog1")
    assertEquals(storeToChangelog.get("test2").getOrElse(""), "mychangelog2")
  }
}
