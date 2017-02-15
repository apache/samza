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

package org.apache.samza.test;

import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import scala.Option$;

import java.io.File;
import java.util.Properties;

public abstract class StandaloneIntegrationTestHarness extends AbstractIntegrationTestHarness {
  private KafkaProducer producer = null;
  private KafkaConsumer consumer = null;

  @Override
  public void setUp() {
    super.setUp();
    producer = TestUtils.createNewProducer(
        bootstrapServers(),
        1,
        60 * 1000L,
        1024L * 1024L,
        0,
        0L,
        5 * 1000L,
        SecurityProtocol.PLAINTEXT,
        null,
        Option$.MODULE$.<Properties>apply(new Properties()),
        new StringSerializer(),
        new ByteArraySerializer(),
        Option$.MODULE$.<Properties>apply(new Properties()));

    consumer = TestUtils.createNewConsumer(
        bootstrapServers(),
        "group",
        "earliest",
        4096L,
        "org.apache.kafka.clients.consumer.RangeAssignor",
        30000,
        SecurityProtocol.PLAINTEXT,
        Option$.MODULE$.<File>empty(),
        Option$.MODULE$.<Properties>empty(),
        Option$.MODULE$.<Properties>empty());
  }

  @Override
  public void tearDown() {
    consumer.close();
    producer.close();
    super.tearDown();
  }

  public KafkaProducer getKafkaProducer() {
    return producer;
  }

  public KafkaConsumer getKafkaConsumer() {
    return consumer;
  }
}

