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

import java.util.Arrays
import org.junit.Assert._
import org.junit.Test
import java.nio.ByteBuffer

class TestByteBufferSerde {
  @Test
  def testSerde {
    val serde = new ByteBufferSerde
    assertNull(serde.toBytes(null))
    assertNull(serde.fromBytes(null))

    val bytes = "A lazy way of creating a byte array".getBytes()
    val byteBuffer = ByteBuffer.wrap(bytes)
    byteBuffer.mark()
    assertArrayEquals(serde.toBytes(byteBuffer), bytes)
    byteBuffer.reset()
    assertEquals(serde.fromBytes(bytes), byteBuffer)
  }

  @Test
  def testSerializationPreservesInput {
    val serde = new ByteBufferSerde
    val bytes = "A lazy way of creating a byte array".getBytes()
    val byteBuffer = ByteBuffer.wrap(bytes)
    byteBuffer.get() // advance position by 1
    serde.toBytes(byteBuffer)

    assertEquals(byteBuffer.capacity(), byteBuffer.limit())
    assertEquals(1, byteBuffer.position())
  }
}