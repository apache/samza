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
 *
 */
package org.apache.samza.operators.impl;

import org.apache.samza.serializers.StringSerde;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 *
 */
public class TestTimeSeriesKeySerde {

  @Test
  public void testStringTimeSeriesKey() {
    TimeSeriesKey<String> timeSeriesKey = new TimeSeriesKey<>("test", 1, 23);
    TimeSeriesKeySerde<String> serde = new TimeSeriesKeySerde<>(new StringSerde("UTF-8"));

    byte[] serializedBytes = serde.toBytes(timeSeriesKey);
    TimeSeriesKey<String> deserializedTimeSeriesKey = serde.fromBytes(serializedBytes);

    assertEquals(timeSeriesKey.getKey(), deserializedTimeSeriesKey.getKey());
    assertEquals(timeSeriesKey.getSeqNum(), deserializedTimeSeriesKey.getSeqNum());
    assertEquals(timeSeriesKey.getTimestamp(), deserializedTimeSeriesKey.getTimestamp());
    assertEquals(timeSeriesKey, deserializedTimeSeriesKey);
  }

  @Test
  public void testNullTimeSeriesKey() {
    TimeSeriesKey<String> nullTimeSeriesKey = new TimeSeriesKey<>(null, 1, 23);
    TimeSeriesKeySerde<String> serde = new TimeSeriesKeySerde<>(new StringSerde("UTF-8"));
    byte[] serializedBytes = serde.toBytes(nullTimeSeriesKey);
    TimeSeriesKey<String> deserializedTimeSeriesKey = serde.fromBytes(serializedBytes);

    assertEquals(nullTimeSeriesKey.getKey(), deserializedTimeSeriesKey.getKey());
    assertEquals(nullTimeSeriesKey.getSeqNum(), deserializedTimeSeriesKey.getSeqNum());
    assertEquals(nullTimeSeriesKey.getTimestamp(), deserializedTimeSeriesKey.getTimestamp());

    assertEquals(nullTimeSeriesKey, deserializedTimeSeriesKey);
  }

}
