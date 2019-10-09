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

import java.nio.BufferUnderflowException;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestUUIDSerde {
  private UUIDSerde serde = new UUIDSerde();

  @Test
  public void testUUIDSerde() {
    UUID uuid = new UUID(13, 42);
    byte[] bytes = serde.toBytes(uuid);
    assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 42}, bytes);
    assertEquals(uuid, serde.fromBytes(bytes));
  }

  @Test
  public void testToBytesWhenNull() {
    assertNull(serde.toBytes(null));
  }

  @Test
  public void testFromBytesWhenNull() {
    assertNull(serde.fromBytes(null));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testFromBytesWhenInvalid() {
    serde.fromBytes(new byte[]{0});
  }
}
