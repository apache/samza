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

import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.KafkaUtilJava;


/**
 * Basic implementation of {@link ConsumerRecordHandler} which mostly just does passthrough of fields from
 * {@link ConsumerRecord} to {@link IncomingMessageEnvelope}.
 */
public class BasicConsumerRecordHandler implements ConsumerRecordHandler {
  private final Clock clock;

  public BasicConsumerRecordHandler(Clock clock) {
    this.clock = clock;
  }

  @Override
  public IncomingMessageEnvelope onNewRecord(ConsumerRecord<?, ?> consumerRecord,
      SystemStreamPartition systemStreamPartition) {
    return new IncomingMessageEnvelope(systemStreamPartition, String.valueOf(consumerRecord.offset()),
        consumerRecord.key(), consumerRecord.value(), KafkaUtilJava.getRecordSize(consumerRecord),
        consumerRecord.timestamp(), Instant.now().toEpochMilli());
  }
}
