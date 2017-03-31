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

import java.util.Properties
import org.apache.samza.config.factories.PropertiesConfigFactory
import org.junit.Assert._
import org.junit.Test
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.Before

class TestKafkaConfig {
  
  var props : Properties = new Properties
  val SYSTEM_NAME = "kafka";
  val KAFKA_PRODUCER_PROPERTY_PREFIX = "systems." + SYSTEM_NAME + ".producer."
  val TEST_CLIENT_ID = "TestClientId"
  val TEST_GROUP_ID = "TestGroupId"
  
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

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)

    val consumerConfig1 = kafkaConfig.getKafkaSystemConsumerConfig(SYSTEM_NAME, "TestClientId1")
    val consumerClientId1 = consumerConfig1.clientId
    val groupId1 = consumerConfig1.groupId
    val consumerConfig2 = kafkaConfig.getKafkaSystemConsumerConfig(SYSTEM_NAME, "TestClientId2")
    val consumerClientId2 = consumerConfig2.clientId
    val groupId2 = consumerConfig2.groupId
    assert(consumerClientId1.equals("TestClientId1"))
    assert(consumerClientId2.equals("TestClientId2"))
    assert(groupId1.startsWith("undefined-samza-consumer-group-"))
    assert(groupId2.startsWith("undefined-samza-consumer-group-"))
    assert(consumerClientId1 != consumerClientId2)
    assert(groupId1 != groupId2)

    val consumerConfig3 = kafkaConfig.getKafkaSystemConsumerConfig(SYSTEM_NAME, TEST_CLIENT_ID, TEST_GROUP_ID)
    val consumerClientId3 = consumerConfig3.clientId
    val groupId3 = consumerConfig3.groupId
    assert(consumerClientId3.equals(TEST_CLIENT_ID))
    assert(groupId3.equals(TEST_GROUP_ID))

    val producerConfig1 = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, "TestClientId1")
    val producerClientId1 = producerConfig1.clientId
    val producerConfig2 = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, "TestClientId2")
    val producerClientId2 = producerConfig2.clientId

    assert(producerClientId1.equals("TestClientId1"))
    assert(producerClientId2.equals("TestClientId2"))
  }

  @Test
  def testStreamLevelFetchSizeOverride() {
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val consumerConfig = kafkaConfig.getKafkaSystemConsumerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    // default fetch size
    assertEquals(1024*1024, consumerConfig.fetchMessageMaxBytes)

    props.setProperty("systems." + SYSTEM_NAME + ".consumer.fetch.message.max.bytes", "262144")
    val mapConfig1 = new MapConfig(props.asScala.asJava)
    val kafkaConfig1 = new KafkaConfig(mapConfig1)
    val consumerConfig1 = kafkaConfig1.getKafkaSystemConsumerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    // shared fetch size
    assertEquals(512*512, consumerConfig1.fetchMessageMaxBytes)
    
    props.setProperty("systems." + SYSTEM_NAME + ".streams.topic1.consumer.fetch.message.max.bytes", "65536")
    val mapConfig2 = new MapConfig(props.asScala.asJava)
    val kafkaConfig2 = new KafkaConfig(mapConfig2)
    val consumerConfig2 = kafkaConfig2.getFetchMessageMaxBytesTopics(SYSTEM_NAME)
    // topic fetch size
    assertEquals(256*256, consumerConfig2 getOrElse ("topic1", 1024*1024))

    // default samza.fetch.threshold.bytes
    val mapConfig3 = new MapConfig(props.asScala.asJava)
    val kafkaConfig3 = new KafkaConfig(mapConfig3)
    assertTrue(kafkaConfig3.getConsumerFetchThresholdBytes("kafka").isEmpty)

    props.setProperty("systems.kafka.samza.fetch.threshold.bytes", "65536")
    val mapConfig4 = new MapConfig(props.asScala.asJava)
    val kafkaConfig4 = new KafkaConfig(mapConfig4)
    assertEquals("65536", kafkaConfig4.getConsumerFetchThresholdBytes("kafka").get)
  }

  @Test
  def testChangeLogProperties() {
    props.setProperty("systems." + SYSTEM_NAME + ".samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory")
    props.setProperty("stores.test1.changelog", "kafka.mychangelog1")
    props.setProperty("stores.test2.changelog", "kafka.mychangelog2")
    props.setProperty("job.changelog.system", "kafka")
    props.setProperty("stores.test3.changelog", "otherstream")
    props.setProperty("stores.test1.changelog.kafka.cleanup.policy", "delete")
    
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test1").getProperty("cleanup.policy"), "delete")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test2").getProperty("cleanup.policy"), "compact")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test3").getProperty("cleanup.policy"), "compact")
    val storeToChangelog = kafkaConfig.getKafkaChangelogEnabledStores()
    assertEquals("mychangelog1", storeToChangelog.get("test1").getOrElse(""))
    assertEquals("mychangelog2", storeToChangelog.get("test2").getOrElse(""))
    assertEquals("otherstream", storeToChangelog.get("test3").getOrElse(""))
  }
  
  @Test
  def testDefaultValuesForProducerProperties() {
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
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
    
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    val producerProperties = kafkaProducerConfig.getProducerProperties
    
    assertEquals(expectedValue, producerProperties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION))
  }
  
  @Test
  def testRetriesOverride() {
    val expectedValue = "200";
    
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.RETRIES_CONFIG, expectedValue);
    
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    val producerProperties = kafkaProducerConfig.getProducerProperties
    
    assertEquals(expectedValue, producerProperties.get(ProducerConfig.RETRIES_CONFIG))
  }
  
  @Test(expected = classOf[NumberFormatException])
  def testMaxInFlightRequestsPerConnectionWrongNumberFormat() {
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "Samza");
    
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    kafkaProducerConfig.getProducerProperties
  }
  
  @Test(expected = classOf[NumberFormatException])
  def testRetriesWrongNumberFormat() {
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.RETRIES_CONFIG, "Samza");
    
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    kafkaProducerConfig.getProducerProperties
  }
}
