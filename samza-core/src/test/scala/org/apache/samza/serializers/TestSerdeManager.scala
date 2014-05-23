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
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemStream
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition

class TestSerdeManager {
  @Test
  def testNullSerializationReturnsIdenticalObject {
    val original = new OutgoingMessageEnvelope(new SystemStream("my-system", "my-stream"), "message")
    val serialized = new SerdeManager().toBytes(original)
    assertSame(original, serialized)
  }

  @Test
  def testNullDeserializationReturnsIdenticalObject {
    val ssp = new SystemStreamPartition("my-system", "my-stream", new Partition(0))
    val original = new IncomingMessageEnvelope(ssp, "123", null, "message")
    val deserialized = new SerdeManager().fromBytes(original)
    assertSame(original, deserialized)
  }
}