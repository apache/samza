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
package org.apache.samza.serializers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestSerializableSerde {
  @Test
  public void testSerializableSerde() {
    SerializableSerde<String> serde = new SerializableSerde<>();
    assertNull(serde.toBytes(null));
    assertNull(serde.fromBytes(null));

    String obj = "String is serializable";

    // Serialized string is prefix + string itself
    List<Byte> expectedBytesList = new ArrayList<>(
        Arrays.asList((byte) 0xAC, (byte) 0xED, (byte) 0x00, (byte) 0x05, (byte) 0x74, (byte) 0x00, (byte) 0x16));
    for (byte b : obj.getBytes(StandardCharsets.UTF_8)) {
      expectedBytesList.add(b);
    }
    // need to unbox to primitive byte
    byte[] expected = new byte[expectedBytesList.size()];
    for (int i = 0; i < expectedBytesList.size(); i++) {
      expected[i] = expectedBytesList.get(i);
    }

    byte[] bytes = serde.toBytes(obj);

    assertArrayEquals(expected, bytes);

    String objRoundTrip = serde.fromBytes(bytes);
    assertEquals(obj, objRoundTrip);
  }
}
