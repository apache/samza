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
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.SystemStream
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class TestKafkaSystemFactory {
  @Test
  def testFailWhenNoSerdeDefined {
    val producerFactory = new KafkaSystemFactory
    try {
      producerFactory.getProducer(
        "test",
        new MapConfig(Map[String, String]()),
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
      "streams.test.serde" -> "failme"))
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
      "serializers.registry.json.class" -> "samza.serializers.JsonSerdeFactory"))
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
}
