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

import org.junit.Assert._
import org.junit.Test
import java.net.URI
import java.io.File
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.samza.config.factories.PropertiesConfigFactory

class TestKafkaConfig {

  @Test
  def testIdGeneration = {
    val factory = new PropertiesConfigFactory()
    val props = new Properties
    props.setProperty(" systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory")
    props.setProperty( "systems.kafka.consumer.zookeeper.connect", "localhost:2181/")
    props.setProperty( "systems.kafka.producer.metadata.broker.list", "localhost:9092")

    val mapConfig = new MapConfig(props.toMap[String, String])
    val kafkaConfig = new KafkaConfig(mapConfig)
  
    val consumerConfig1 = kafkaConfig.getKafkaSystemConsumerConfig("kafka")
    val consumerClientId1 = consumerConfig1.clientId
    val groupId1 = consumerConfig1.groupId
    val consumerConfig2 = kafkaConfig.getKafkaSystemConsumerConfig("kafka")
    val consumerClientId2 = consumerConfig2.clientId
    val groupId2 = consumerConfig2.groupId
    assert( consumerClientId1.startsWith("undefined-samza-consumer-"))
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

    assert( producerClientId1.startsWith("undefined-samza-producer-"))
    assert(producerClientId2.startsWith("undefined-samza-producer-"))
    assert(producerClientId1 != producerClientId2)

    val producerConfig3 = kafkaConfig.getKafkaSystemProducerConfig("kafka", "TestClientId")
    val producerClientId3 = producerConfig3.clientId
    assert(producerClientId3 == "TestClientId")

  }
}
