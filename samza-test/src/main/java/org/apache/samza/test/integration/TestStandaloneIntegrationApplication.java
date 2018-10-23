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
package org.apache.samza.test.integration;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Acts as a pass through filter for all the events from a input stream.
 */
public class TestStandaloneIntegrationApplication implements StreamApplication {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestStandaloneIntegrationApplication.class);

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    String systemName = "testSystemName";
    String inputStreamName = appDescriptor.getConfig().get("input.stream.name");
    String outputStreamName = "standaloneIntegrationTestKafkaOutputTopic";
    LOGGER.info("Publishing message from: {} to: {}.", inputStreamName, outputStreamName);
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(systemName);

    KVSerde<Object, Object> noOpSerde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
    KafkaInputDescriptor<KV<Object, Object>> isd =
        kafkaSystemDescriptor.getInputDescriptor(inputStreamName, noOpSerde);
    KafkaOutputDescriptor<KV<Object, Object>> osd =
        kafkaSystemDescriptor.getOutputDescriptor(outputStreamName, noOpSerde);
    appDescriptor.getInputStream(isd).sendTo(appDescriptor.getOutputStream(osd));
  }
}
