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

import org.junit.Assert._
import org.junit.Test

class TestIntegerSerde {
  @Test
  def testIntegerSerde {
    val serde = new IntegerSerde
    assertEquals(null, serde.toBytes(null))
    assertEquals(null, serde.fromBytes(null))

    val fooBar = 37
    val fooBarBytes = serde.toBytes(fooBar)
    assertArrayEquals(Array[Byte](0, 0, 0, 37), fooBarBytes)
    assertEquals(fooBar, serde.fromBytes(fooBarBytes))
  }
}
