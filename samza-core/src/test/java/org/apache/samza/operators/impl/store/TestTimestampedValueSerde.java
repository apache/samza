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
package org.apache.samza.operators.impl.store;

import org.apache.samza.serializers.ByteSerde;
import org.apache.samza.serializers.IntegerSerde;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class TestTimestampedValueSerde {

  @Test
  public void testEmptyValueDeserialization() {
    byte[] bytesWithNoValue = new byte[8];
    ByteBuffer.wrap(bytesWithNoValue).putLong(1234L);
    TimestampedValueSerde<byte[]> timestampedValueSerde = new TimestampedValueSerde<>(new ByteSerde());
    TimestampedValue<byte[]> timestampedValue = timestampedValueSerde.fromBytes(bytesWithNoValue);
    assertEquals(1234L, timestampedValue.getTimestamp());
    assertEquals(0, timestampedValue.getValue().length);
  }

  @Test
  public void testEmptyValueSerialization() {
    byte[] expectedBytes = new byte[8];
    ByteBuffer.wrap(expectedBytes).putLong(1234L);

    TimestampedValueSerde<Integer> timestampedValueSerde = new TimestampedValueSerde<>(new IntegerSerde());
    TimestampedValue<Integer> timestampedValue = new TimestampedValue<>(null, 1234L);
    assertTrue(Arrays.equals(expectedBytes, timestampedValueSerde.toBytes(timestampedValue)));
  }
}
