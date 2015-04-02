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

import kafka.utils.VerifiableProperties

object KafkaSerdeConfig {
  // kafka serde config constants
  val ENCODER = SerializerConfig.SERIALIZER_PREFIX + ".encoder"
  val DECODER = SerializerConfig.SERIALIZER_PREFIX + ".decoder"

  implicit def Config2KafkaSerde(config: Config) = new KafkaSerdeConfig(config)
}

class KafkaSerdeConfig(config: Config) extends ScalaMapConfig(config) {
  def getKafkaEncoder(serializer: String) =
    getOption(KafkaSerdeConfig.ENCODER format serializer)

  def getKafkaDecoder(serializer: String) =
    getOption(KafkaSerdeConfig.DECODER format serializer)

  def getKafkaProperties(serializer: String) = {
    val properties = new Properties();
    val prefix = SerializerConfig.SERIALIZER_PREFIX format serializer + "."
    properties.putAll(config.subset(prefix, true))

    new VerifiableProperties(properties);
  }
}
