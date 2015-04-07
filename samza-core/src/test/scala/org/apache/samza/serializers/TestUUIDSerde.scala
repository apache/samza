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

package org.apache.samza.serializers

import java.nio.BufferUnderflowException
import java.util.UUID

import org.junit.Assert._
import org.junit.Test

class TestUUIDSerde {
  private val serde = new UUIDSerde

  @Test
  def testUUIDSerde {
    val uuid = new UUID(13, 42)
    val bytes = serde.toBytes(uuid)
    assertArrayEquals(Array[Byte](0, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 42), bytes)
    assertEquals(uuid, serde.fromBytes(bytes))
  }

  @Test
  def testToBytesWhenNull {
    assertEquals(null, serde.toBytes(null))
  }

  @Test
  def testFromBytesWhenNull {
    assertEquals(null, serde.fromBytes(null))
  }

  @Test(expected = classOf[BufferUnderflowException])
  def testFromBytesWhenInvalid {
    assertEquals(null, serde.fromBytes(Array[Byte](0)))
  }
}
