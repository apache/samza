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
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;


public class TestBasicConsumerRecordHandler {
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 1);
  private static final long PLAIN_OFFSET = 123;
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final Instant RECORD_TIMESTAMP = Instant.ofEpochMilli(10000);
  private static final Instant NOW = Instant.ofEpochMilli(10001);
  private static final SystemStreamPartition SSP =
      new SystemStreamPartition("system", TOPIC_PARTITION.topic(), new Partition(TOPIC_PARTITION.partition()));

  @Mock
  private Clock clock;
  @Mock
  private ConsumerRecord<String, String> consumerRecord;

  private BasicConsumerRecordHandler basicConsumerRecordHandler;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(this.clock.currentTimeMillis()).thenReturn(NOW.toEpochMilli());

    when(this.consumerRecord.partition()).thenReturn(TOPIC_PARTITION.partition());
    when(this.consumerRecord.topic()).thenReturn(TOPIC_PARTITION.topic());
    when(this.consumerRecord.offset()).thenReturn(PLAIN_OFFSET);
    when(this.consumerRecord.key()).thenReturn(KEY);
    when(this.consumerRecord.value()).thenReturn(VALUE);
    when(this.consumerRecord.serializedKeySize()).thenReturn(KEY.length());
    when(this.consumerRecord.serializedValueSize()).thenReturn(VALUE.length());
    when(this.consumerRecord.timestamp()).thenReturn(RECORD_TIMESTAMP.toEpochMilli());

    this.basicConsumerRecordHandler = new BasicConsumerRecordHandler(this.clock);
  }

  @Test
  public void testOnNewRecord() {
    IncomingMessageEnvelope expected =
        new IncomingMessageEnvelope(SSP, Long.toString(PLAIN_OFFSET), KEY, VALUE, KEY.length() + VALUE.length(),
            RECORD_TIMESTAMP.toEpochMilli(), NOW.toEpochMilli());
    assertEquals(this.basicConsumerRecordHandler.onNewRecord(this.consumerRecord, SSP), expected);
  }
}