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
package org.apache.samza.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestKafkaUtilJava {
  @Test
  public void testGetRecordSize() {
    String key = "key";
    String value = "value";
    ConsumerRecord<String, String> consumerRecord = mock(ConsumerRecord.class);

    // null key and value
    when(consumerRecord.key()).thenReturn(null);
    when(consumerRecord.value()).thenReturn(null);
    assertEquals(0, KafkaUtilJava.getRecordSize(consumerRecord));

    // null key, non-null value
    when(consumerRecord.key()).thenReturn(null);
    when(consumerRecord.value()).thenReturn(value);
    when(consumerRecord.serializedValueSize()).thenReturn(value.length());
    assertEquals(value.length(), KafkaUtilJava.getRecordSize(consumerRecord));

    // non-null key, null value (this case seems odd, but the ConsumerRecord API seems to allow it)
    when(consumerRecord.key()).thenReturn(key);
    when(consumerRecord.serializedKeySize()).thenReturn(key.length());
    when(consumerRecord.value()).thenReturn(null);
    assertEquals(key.length(), KafkaUtilJava.getRecordSize(consumerRecord));

    // non-null key, non-null value
    when(consumerRecord.key()).thenReturn(key);
    when(consumerRecord.serializedKeySize()).thenReturn(key.length());
    when(consumerRecord.value()).thenReturn(value);
    when(consumerRecord.serializedValueSize()).thenReturn(value.length());
    assertEquals(key.length() + value.length(), KafkaUtilJava.getRecordSize(consumerRecord));
  }
}