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

import org.apache.samza.SamzaException
import org.apache.samza.config.MapConfig
import org.apache.samza.config.StorageConfig
import org.apache.samza.metrics.MetricsRegistryMap
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class TestKafkaSystemFactory {
  @Test
  def testFailWhenNoSerdeDefined {
    val producerFactory = new KafkaSystemFactory
    try {
      producerFactory.getProducer(
        "test",
        new MapConfig(Map[String, String]().asJava),
        new MetricsRegistryMap)
      fail("Expected to get a Samza exception.")
    } catch {
      case e: SamzaException => None // expected
      case e: Exception => fail("Expected SamzaException, but got " + e)
    }
  }

  @Test
  def testFailWhenSerdeIsInvalid {
    val producerFactory = new KafkaSystemFactory
    val config = new MapConfig(Map[String, String](
      "streams.test.serde" -> "failme").asJava)
    try {
      producerFactory.getProducer(
        "test",
        config,
        new MetricsRegistryMap)
      fail("Expected to get a Samza exception.")
    } catch {
      case e: SamzaException => None // expected
      case e: Exception => fail("Expected SamzaException, but got " + e)
    }
  }

  @Test
  def testHappyPath {
    val producerFactory = new KafkaSystemFactory
    val config = new MapConfig(Map[String, String](
      "job.name" -> "test",
      "systems.test.producer.bootstrap.servers" -> "",
      "systems.test.samza.key.serde" -> "json",
      "systems.test.samza.msg.serde" -> "json",
      "serializers.registry.json.class" -> "samza.serializers.JsonSerdeFactory").asJava)
    var producer = producerFactory.getProducer(
      "test",
      config,
      new MetricsRegistryMap)
    assertNotNull(producer)
    assertTrue(producer.isInstanceOf[KafkaSystemProducer])
    producer = producerFactory.getProducer(
      "test",
      config,
      new MetricsRegistryMap)
    assertNotNull(producer)
    assertTrue(producer.isInstanceOf[KafkaSystemProducer])
  }

  @Test
  def testInjectedProducerProps {
    val configMap = Map[String, String](
      StorageConfig.FACTORY.format("system1") -> "some.factory.Class",
      StorageConfig.CHANGELOG_STREAM.format("system1") -> "system1.stream1",
      StorageConfig.FACTORY.format("system2") -> "some.factory.Class")
    val config = new MapConfig(configMap.asJava)
    assertEquals(Map[String, String](), KafkaSystemFactory.getInjectedProducerProperties("system3", config))
    assertEquals(Map[String, String](), KafkaSystemFactory.getInjectedProducerProperties("system2", config))
    assertEquals(Map[String, String]("compression.type" -> "none"), KafkaSystemFactory.getInjectedProducerProperties("system1", config))
  }
}
