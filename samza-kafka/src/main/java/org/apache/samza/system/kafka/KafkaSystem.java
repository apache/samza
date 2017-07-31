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
package org.apache.samza.system.kafka;

import org.apache.samza.config.Config;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.operators.IOSystem;


public class KafkaSystem implements IOSystem {

  private static Map<String, KafkaSystem> kafkaSystems = new HashMap<>();

  private final String systemName;

  private KafkaSystem(String systemName) {
    this.systemName = systemName;
  }

  public static KafkaSystem create(String systemId) {
    if (kafkaSystems.containsKey(systemId)) {
      return kafkaSystems.get(systemId);
    }
    KafkaSystem system = new KafkaSystem(systemId);
    kafkaSystems.putIfAbsent(systemId, system);
    return kafkaSystems.get(systemId);
  }

  public KafkaSystem withBootstrapServers(String brokerList) {
    return this;
  }

  public KafkaSystem withConsumerProperties(Config consumerConfig) {
    return this;
  }

  public KafkaSystem withProducerProperties(Config producerConfig) {
    return this;
  }

  @Override
  public String getSystemName() {
    return this.systemName;
  }
}
