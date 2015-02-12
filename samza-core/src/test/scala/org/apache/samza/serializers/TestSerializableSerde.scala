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

class TestSerializableSerde {
  @Test
  def testSerializableSerde {
    val serde = new SerializableSerde[String]
    assertNull(serde.toBytes(null))
    assertNull(serde.fromBytes(null))
    
    val obj = "String is serializable"

    // Serialized string is prefix + string itself
    val prefix = Array(0xAC, 0xED, 0x00, 0x05, 0x74, 0x00, 0x16).map(_.toByte)
    val expected = (prefix ++ obj.getBytes("UTF-8"))
    
    val bytes = serde.toBytes(obj)

    assertArrayEquals(expected, bytes)

    val objRoundTrip:String = serde.fromBytes(bytes)
    assertEquals(obj, objRoundTrip)
  }
}
