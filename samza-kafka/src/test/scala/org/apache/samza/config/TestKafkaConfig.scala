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
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.Before
import org.junit.BeforeClass

class TestKafkaConfig {
  
  var props : Properties = new Properties
  val SYSTEM_NAME = "kafka";
  val KAFKA_PRODUCER_PROPERTY_PREFIX = "systems." + SYSTEM_NAME + ".producer."
  
  @Before
  def setupProperties() {
    props = new Properties
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + "bootstrap.servers", "localhost:9092")
    props.setProperty("systems." + SYSTEM_NAME + ".consumer.zookeeper.connect", "localhost:2181/")
  }
  
  @Test
  def testIdGeneration = {
    val factory = new PropertiesConfigFactory()
    props.setProperty("systems." + SYSTEM_NAME + ".samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory")

    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)

    val consumerConfig1 = kafkaConfig.getKafkaSystemConsumerConfig(SYSTEM_NAME)
    val consumerClientId1 = consumerConfig1.clientId
    val groupId1 = consumerConfig1.groupId
    val consumerConfig2 = kafkaConfig.getKafkaSystemConsumerConfig(SYSTEM_NAME)
    val consumerClientId2 = consumerConfig2.clientId
    val groupId2 = consumerConfig2.groupId
    assert(consumerClientId1.startsWith("undefined-samza-consumer-"))
    assert(consumerClientId2.startsWith("undefined-samza-consumer-"))
    assert(groupId1.startsWith("undefined-samza-consumer-group-"))
    assert(groupId2.startsWith("undefined-samza-consumer-group-"))
    assert(consumerClientId1 != consumerClientId2)
    assert(groupId1 != groupId2)

    val consumerConfig3 = kafkaConfig.getKafkaSystemConsumerConfig(SYSTEM_NAME, "TestClientId", "TestGroupId")
    val consumerClientId3 = consumerConfig3.clientId
    val groupId3 = consumerConfig3.groupId
    assert(consumerClientId3 == "TestClientId")
    assert(groupId3 == "TestGroupId")

    val producerConfig1 = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME)
    val producerClientId1 = producerConfig1.clientId
    val producerConfig2 = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME)
    val producerClientId2 = producerConfig2.clientId

    assert(producerClientId1.startsWith("undefined-samza-producer-"))
    assert(producerClientId2.startsWith("undefined-samza-producer-"))
    assert(producerClientId1 != producerClientId2)

    val producerConfig3 = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, "TestClientId")
    val producerClientId3 = producerConfig3.clientId
    assert(producerClientId3 == "TestClientId")

  }

  @Test
  def testStreamLevelFetchSizeOverride() {
    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    val consumerConfig = kafkaConfig.getKafkaSystemConsumerConfig(SYSTEM_NAME)
    // default fetch size
    assertEquals(1024*1024, consumerConfig.fetchMessageMaxBytes)

    props.setProperty("systems." + SYSTEM_NAME + ".consumer.fetch.message.max.bytes", "262144")
    val mapConfig1 = new MapConfig(props.toMap[String, String])
    val kafkaConfig1 = new KafkaConfig(mapConfig1)
    val consumerConfig1 = kafkaConfig1.getKafkaSystemConsumerConfig(SYSTEM_NAME)
    // shared fetch size
    assertEquals(512*512, consumerConfig1.fetchMessageMaxBytes)
    
    props.setProperty("systems." + SYSTEM_NAME + ".streams.topic1.consumer.fetch.message.max.bytes", "65536")
    val mapConfig2 = new MapConfig(props.toMap[String, String])
    val kafkaConfig2 = new KafkaConfig(mapConfig2)
    val consumerConfig2 = kafkaConfig2.getFetchMessageMaxBytesTopics(SYSTEM_NAME)
    // topic fetch size
    assertEquals(256*256, consumerConfig2 getOrElse ("topic1", 1024*1024))

    // default samza.fetch.threshold.bytes
    val mapConfig3 = new MapConfig(props.toMap[String, String])
    val kafkaConfig3 = new KafkaConfig(mapConfig3)
    assertTrue(kafkaConfig3.getConsumerFetchThresholdBytes("kafka").isEmpty)

    props.setProperty("systems.kafka.samza.fetch.threshold.bytes", "65536")
    val mapConfig4 = new MapConfig(props.toMap[String, String])
    val kafkaConfig4 = new KafkaConfig(mapConfig4)
    assertEquals("65536", kafkaConfig4.getConsumerFetchThresholdBytes("kafka").get)
  }

  @Test
  def testChangeLogProperties() {
    props.setProperty("systems." + SYSTEM_NAME + ".samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory")
    props.setProperty("stores.test1.changelog", "kafka.mychangelog1")
    props.setProperty("stores.test2.changelog", "kafka.mychangelog2")
    props.setProperty("stores.test1.changelog.kafka.cleanup.policy", "delete")
    
    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test1").getProperty("cleanup.policy"), "delete")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test2").getProperty("cleanup.policy"), "compact")
    val storeToChangelog = kafkaConfig.getKafkaChangelogEnabledStores()
    assertEquals(storeToChangelog.get("test1").getOrElse(""), "mychangelog1")
    assertEquals(storeToChangelog.get("test2").getOrElse(""), "mychangelog2")
  }
  
  @Test
  def testDefaultValuesForProducerProperties() {
    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME)
    val producerProperties = kafkaProducerConfig.getProducerProperties
    
    assertEquals(classOf[ByteArraySerializer].getCanonicalName, producerProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
    assertEquals(classOf[ByteArraySerializer].getCanonicalName, producerProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
    assertEquals(kafkaProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT, producerProperties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION))
    assertEquals(kafkaProducerConfig.RETRIES_DEFAULT, producerProperties.get(ProducerConfig.RETRIES_CONFIG))
  }

  @Test
  def testMaxInFlightRequestsPerConnectionOverride() {
    val expectedValue = "200";
    
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, expectedValue);
    
    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME)
    val producerProperties = kafkaProducerConfig.getProducerProperties
    
    assertEquals(expectedValue, producerProperties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION))
  }
  
  @Test
  def testRetriesOverride() {
    val expectedValue = "200";
    
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.RETRIES_CONFIG, expectedValue);
    
    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME)
    val producerProperties = kafkaProducerConfig.getProducerProperties
    
    assertEquals(expectedValue, producerProperties.get(ProducerConfig.RETRIES_CONFIG))
  }
  
  @Test(expected = classOf[NumberFormatException])
  def testMaxInFlightRequestsPerConnectionWrongNumberFormat() {
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "Samza");
    
    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME)
    kafkaProducerConfig.getProducerProperties
  }
  
  @Test(expected = classOf[NumberFormatException])
  def testRetriesWrongNumberFormat() {
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.RETRIES_CONFIG, "Samza");
    
    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME)
    kafkaProducerConfig.getProducerProperties
  }
}
