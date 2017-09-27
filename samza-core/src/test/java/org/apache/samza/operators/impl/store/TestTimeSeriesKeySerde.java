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
package org.apache.samza.operators.impl.store;

import com.google.common.primitives.Longs;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.serializers.StringSerde;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestTimeSeriesKeySerde {

  @Test
  public void testStringTimeSeriesKey() {
    TimeSeriesKey<String> storeKey = new TimeSeriesKey<>("test", 1, 23);
    TimeSeriesKeySerde<String> serde = new TimeSeriesKeySerde<>(new StringSerde("UTF-8"));

    byte[] serializedBytes = serde.toBytes(storeKey);
    TimeSeriesKey<String> deserializedTimeSeriesKey = serde.fromBytes(serializedBytes);

    assertEquals(storeKey.getKey(), deserializedTimeSeriesKey.getKey());
    assertEquals(storeKey.getSeqNum(), deserializedTimeSeriesKey.getSeqNum());
    assertEquals(storeKey.getTimestamp(), deserializedTimeSeriesKey.getTimestamp());
    assertEquals(storeKey, deserializedTimeSeriesKey);
  }

  @Test
  public void testNullTimeSeriesKey() {
    TimeSeriesKey<String> storeKey = new TimeSeriesKey<>(null, 1, 23);
    TimeSeriesKeySerde<String> serde = new TimeSeriesKeySerde<>(new StringSerde("UTF-8"));
    byte[] serializedBytes = serde.toBytes(storeKey);
    TimeSeriesKey<String> deserializedTimeSeriesKey = serde.fromBytes(serializedBytes);

    assertEquals(storeKey.getKey(), deserializedTimeSeriesKey.getKey());
    assertEquals(storeKey.getSeqNum(), deserializedTimeSeriesKey.getSeqNum());
    assertEquals(storeKey.getTimestamp(), deserializedTimeSeriesKey.getTimestamp());

    assertEquals(storeKey, deserializedTimeSeriesKey);
  }

  @Test
  public void testLongTimeSeriesKey() {
    TimeSeriesKey<Long> storeKey = new TimeSeriesKey<>(30L, 1, 23);
    TimeSeriesKeySerde<Long> serde = new TimeSeriesKeySerde<>(new LongSerde());
    byte[] serializedBytes = serde.toBytes(storeKey);
    TimeSeriesKey<Long> deserializedTimeSeriesKey = serde.fromBytes(serializedBytes);

    assertEquals(storeKey.getKey(), deserializedTimeSeriesKey.getKey());
    assertEquals(storeKey.getSeqNum(), deserializedTimeSeriesKey.getSeqNum());
    assertEquals(storeKey.getTimestamp(), deserializedTimeSeriesKey.getTimestamp());

    assertEquals(storeKey, deserializedTimeSeriesKey);
  }

  @Test
  public void longBehavior() {
    byte version = 0x03;
    long versionInt = 0x13FFFFFFFFFFFFFFl;
    byte[] bytes1 = Longs.toByteArray(versionInt);
    long val1 = (0xffffffffffffffffl & versionInt) & version << 56;
    byte[] bytes = Longs.toByteArray(val1);


  }

}
