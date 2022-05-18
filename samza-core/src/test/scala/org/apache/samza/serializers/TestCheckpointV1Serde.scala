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

import com.fasterxml.jackson.databind.ObjectMapper

import java.util
import org.apache.samza.Partition
import org.apache.samza.checkpoint.CheckpointV1
import org.apache.samza.container.TaskName
import org.apache.samza.system.SystemStreamPartition
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class TestCheckpointV1Serde {
  @Test
  def testExactlyOneOffset {
    val serde = new CheckpointV1Serde
    var offsets = Map[SystemStreamPartition, String]()
    val systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(777))
    offsets += systemStreamPartition -> "1"
    val deserializedOffsets = serde.fromBytes(serde.toBytes(new CheckpointV1(offsets.asJava)))
    assertEquals("1", deserializedOffsets.getOffsets.get(systemStreamPartition))
    assertEquals(1, deserializedOffsets.getOffsets.size)
  }

  @Test
  def testNullCheckpointSerde: Unit = {
    val checkpointBytes = null.asInstanceOf[Array[Byte]]
    val checkpointSerde = new CheckpointV1Serde
    val checkpoint = checkpointSerde.fromBytes(checkpointBytes)
    assertNull(checkpoint)
  }

  @Test
  def testSSPWithKeyBucket {
    // case 1: write and read with serde that is aware of KeyBucket within SSP
    val serde = new CheckpointV1Serde
    var offsets = Map[SystemStreamPartition, String]()
    val ssp = new SystemStreamPartition("test-system", "test-stream",
      new Partition(777), -1)
    offsets += ssp -> "1"
    val deserializedOffsets = serde.fromBytes(serde.toBytes(new CheckpointV1(offsets.asJava)))
    assertEquals("1", deserializedOffsets.getOffsets.get(ssp))
    assertEquals(1, deserializedOffsets.getOffsets.size)

    // case 2: SSP was serialized by serde not aware of KeyBucket - aka did not put keyBucket into serialized form
    val deserializedOffsets2 = serde.fromBytes(toBytesWithoutKeyBucket(new CheckpointV1(offsets.asJava)))
    assertEquals("1", deserializedOffsets2.getOffsets.get(ssp))
    assertEquals(1, deserializedOffsets2.getOffsets.size)

    // case 3: SSP was serialized by serde aware of KeyBucket but deserialized by serde not aware of KeyBucket
    val deserializedOffsets3 = fromBytesWithoutKeyBucket(serde.toBytes(new CheckpointV1(offsets.asJava)))
    assertEquals("1", deserializedOffsets3.getOffsets.get(ssp))
    assertEquals(1, deserializedOffsets3.getOffsets.size)

    // case 4: SSP has keyBucket = 0 (aka not default -1) - serialize by serde NOT aware of keyBucket
    // when serialized with serde not aware of keyBucket, the info about keyBucket is lost,
    // we can only recover the system, stream and partition parts out during deserialization.
    // hence after deser, we need to look for SSP without key bucket
    val sspWithKeyBucket = new SystemStreamPartition("test-system", "test-stream",
      new Partition(777), 0)
    val sspWithoutKeyBucket = new SystemStreamPartition("test-system", "test-stream",
      new Partition(777))
    var offsets1 = Map[SystemStreamPartition, String]()
    offsets1 += sspWithKeyBucket -> "1"

    val deserializedOffsets4 = serde.fromBytes(toBytesWithoutKeyBucket(new CheckpointV1(offsets1.asJava)))
    assertEquals("1", deserializedOffsets4.getOffsets.get(sspWithoutKeyBucket))
    assertEquals(1, deserializedOffsets4.getOffsets.size)

    // case 5: SSP has KeyBucket = 0, serialized by serde aware of keyBucket
    val deserializedOffsets5 = fromBytesWithoutKeyBucket(serde.toBytes(new CheckpointV1(offsets1.asJava)))
    assertEquals("1", deserializedOffsets5.getOffsets.get(sspWithoutKeyBucket))
    assertEquals(1, deserializedOffsets5.getOffsets.size)
  }

  private  def fromBytesWithoutKeyBucket(bytes: Array[Byte]): CheckpointV1 = {
    val jsonMapper = new ObjectMapper()
    val jMap = jsonMapper.readValue(bytes, classOf[util.HashMap[String, util.HashMap[String, String]]])

    def deserializeJSONMap(sspInfo:util.HashMap[String, String]) = {
      val system = sspInfo.get("system")
      val stream = sspInfo.get("stream")
      val partition = sspInfo.get("partition")
      val offset = sspInfo.get("offset")
      new SystemStreamPartition(system, stream, new Partition(partition.toInt)) -> offset
    }
    val cpMap = jMap.values.asScala.map(deserializeJSONMap).toMap
    new CheckpointV1(cpMap.asJava)
  }

  private   def toBytesWithoutKeyBucket(checkpoint: CheckpointV1): Array[Byte] = {
    val jsonMapper = new ObjectMapper()
    val offsets = checkpoint.getOffsets
    val asMap = new util.HashMap[String, util.HashMap[String, String]](offsets.size())

    offsets.asScala.foreach {
      case (ssp, offset) =>
        val jMap = new util.HashMap[String, String](4)
        jMap.put("system", ssp.getSystemStream.getSystem)
        jMap.put("stream", ssp.getSystemStream.getStream)
        jMap.put("partition", ssp.getPartition.getPartitionId.toString)
        jMap.put("offset", offset)

        asMap.put(ssp.toString, jMap)
    }

    jsonMapper.writeValueAsBytes(asMap)
  }
}
