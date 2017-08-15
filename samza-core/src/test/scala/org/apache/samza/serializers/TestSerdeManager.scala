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


import org.apache.samza.system.EndOfStreamMessage
import org.apache.samza.system.WatermarkMessage
import org.junit.Assert._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertEquals
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

  @Test
  def testIntermediateMessageSerde {
    val output = new SystemStream("my-system", "output")
    val intermediate = new SystemStream("my-system", "intermediate")
    val intSerde = (new IntegerSerde).asInstanceOf[Serde[Object]]
    val systemStreamKeySerdes: Map[SystemStream, Serde[Object]] = Map(output -> intSerde)
    val systemStreamMessageSerdes: Map[SystemStream, Serde[Object]] = Map(output -> intSerde)
    val controlMessageKeySerdes: Map[SystemStream, Serde[String]] = Map(intermediate -> new StringSerde("UTF-8"))
    val intermediateMessageSerdes: Map[SystemStream, Serde[Object]] = Map(intermediate -> new IntermediateMessageSerde(intSerde))

    val serdeManager = new SerdeManager(systemStreamKeySerdes = systemStreamKeySerdes,
                                        systemStreamMessageSerdes = systemStreamMessageSerdes,
                                        controlMessageKeySerdes = controlMessageKeySerdes,
                                        intermediateMessageSerdes = intermediateMessageSerdes)

    // test user message sent to output stream
    var outEnvelope = new OutgoingMessageEnvelope(output, 1, 1000)
    var se = serdeManager.toBytes(outEnvelope)
    var inEnvelope = new IncomingMessageEnvelope(new SystemStreamPartition(output, new Partition(0)), "offset", se.getKey, se.getMessage)
    var de = serdeManager.fromBytes(inEnvelope)
    assertEquals(de.getKey, 1)
    assertEquals(de.getMessage, 1000)

    // test user message sent to intermediate stream
    outEnvelope = new OutgoingMessageEnvelope(intermediate, 1, 1000)
    se = serdeManager.toBytes(outEnvelope)
    inEnvelope = new IncomingMessageEnvelope(new SystemStreamPartition(intermediate, new Partition(0)), "offset", se.getKey, se.getMessage)
    de = serdeManager.fromBytes(inEnvelope)
    assertEquals(de.getKey, 1)
    assertEquals(de.getMessage, 1000)

    // test end-of-stream message sent to intermediate stream
    val eosStreamId = "eos-stream"
    val taskName = "task 1"
    val taskCount = 8
    outEnvelope = new OutgoingMessageEnvelope(intermediate, "eos", new EndOfStreamMessage(taskName))
    se = serdeManager.toBytes(outEnvelope)
    inEnvelope = new IncomingMessageEnvelope(new SystemStreamPartition(intermediate, new Partition(0)), "offset", se.getKey, se.getMessage)
    de = serdeManager.fromBytes(inEnvelope)
    assertEquals(de.getKey, "eos")
    val eosMsg = de.getMessage.asInstanceOf[EndOfStreamMessage]
    assertEquals(eosMsg.getTaskName, taskName)

    // test watermark message sent to intermediate stream
    val timestamp = System.currentTimeMillis()
    outEnvelope = new OutgoingMessageEnvelope(intermediate, "watermark", new WatermarkMessage(timestamp, taskName))
    se = serdeManager.toBytes(outEnvelope)
    inEnvelope = new IncomingMessageEnvelope(new SystemStreamPartition(intermediate, new Partition(0)), "offset", se.getKey, se.getMessage)
    de = serdeManager.fromBytes(inEnvelope)
    assertEquals(de.getKey, "watermark")
    val watermarkMsg = de.getMessage.asInstanceOf[WatermarkMessage]
    assertEquals(watermarkMsg.getTimestamp, timestamp)
    assertEquals(watermarkMsg.getTaskName, taskName)
  }
}