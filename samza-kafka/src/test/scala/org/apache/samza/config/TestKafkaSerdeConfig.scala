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

import org.apache.samza.config.KafkaSerdeConfig.Config2KafkaSerde
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class TestKafkaSerdeConfig {
  val MAGIC_VAL = "1000"

  val paramsToTest = List(
    "serializers.registry.test.encoder", "serializers.registry.test.decoder")

  val config = new MapConfig(mapAsJavaMap(paramsToTest.map { m => (m, MAGIC_VAL) }.toMap))

  @Test
  def testKafkaConfigurationIsBackwardsCompatible {
    assertEquals(MAGIC_VAL, config.getKafkaEncoder("test").getOrElse(""))
    assertEquals(MAGIC_VAL, config.getKafkaDecoder("test").getOrElse(""))
  }

  @Test
  def testKafkaConfigurationProperties {
    val properties = config.getKafkaProperties("test")
    assertEquals(MAGIC_VAL, properties.getString("encoder"))
    assertEquals(MAGIC_VAL, properties.getString("decoder"))
  }
}
