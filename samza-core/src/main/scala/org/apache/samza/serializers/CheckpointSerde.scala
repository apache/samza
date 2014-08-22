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

import org.apache.samza.util.Logging
import java.util
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.container.TaskName
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.{SamzaException, Partition}
import org.codehaus.jackson.map.ObjectMapper
import scala.collection.JavaConversions._
import org.codehaus.jackson.`type`.TypeReference

/**
 * Write out the Checkpoint object in JSON.  The underlying map of SSP => Offset cannot be stored directly because
 * JSON only allows strings as map types, so we would need to separately serialize the SSP to a string that doesn't
 * then interfere with JSON's decoding of the overall map.  We'll sidestep the whole issue by turning the
 * map into a list[String] of (System, Stream, Partition, Offset) serializing that.
 */
class CheckpointSerde extends Serde[Checkpoint] with Logging {
  import CheckpointSerde._
  // TODO: Elucidate the CheckpointSerde relationshiop to Serde. Should Serde also have keyTo/FromBytes? Should
  // we just take CheckpointSerde here as interface and have this be JSONCheckpointSerde?
  // TODO: Add more tests.  This class currently only has direct test and is mainly tested by the other checkpoint managers
  val jsonMapper = new ObjectMapper()

  // Jackson absolutely hates Scala types and hidden conversions hate you, so we're going to be very, very
  // explicit about the Java (not Scala) types used here and never let Scala get its grubby little hands
  // on any instance.

  // Store checkpoint as maps keyed of the SSP.toString to the another map of the constituent SSP components
  // and offset.  Jackson can't automatically serialize the SSP since it's not a POJO and this avoids
  // having to wrap it another class while maintaing readability.

  def fromBytes(bytes: Array[Byte]): Checkpoint = {
    try {
      val jMap = jsonMapper.readValue(bytes, classOf[util.HashMap[String, util.HashMap[String, String]]])

      def deserializeJSONMap(m:util.HashMap[String, String]) = {
        require(m.size() == 4, "All JSON-encoded SystemStreamPartitions must have four keys")
        val system = m.get("system")
        require(system != null, "System must be present in JSON-encoded SystemStreamPartition")
        val stream = m.get("stream")
        require(stream != null, "Stream must be present in JSON-encoded SystemStreamPartition")
        val partition = m.get("partition")
        require(partition != null, "Partition must be present in JSON-encoded SystemStreamPartition")
        val offset = m.get("offset")
        require(offset != null, "Offset must be present in JSON-encoded SystemStreamPartition")

        new SystemStreamPartition(system, stream, new Partition(partition.toInt)) -> offset
      }

      val cpMap = jMap.values.map(deserializeJSONMap).toMap

      return new Checkpoint(cpMap)
    }catch {
      case e : Exception =>
        warn("Exception while deserializing checkpoint: " + e)
        debug("Exception detail:", e)
        null
    }
  }

  def toBytes(checkpoint: Checkpoint): Array[Byte] = {
    val offsets = checkpoint.getOffsets
    val asMap = new util.HashMap[String, util.HashMap[String, String]](offsets.size())

    offsets.foreach {
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

  def changelogPartitionMappingFromBytes(bytes: Array[Byte]): util.Map[TaskName, java.lang.Integer] = {
    try {
      jsonMapper.readValue(bytes, PARTITION_MAPPING_TYPEREFERENCE)
    } catch {
      case e : Exception =>
        throw new SamzaException("Exception while deserializing changelog partition mapping", e)
    }
  }

  def changelogPartitionMappingToBytes(mapping: util.Map[TaskName, java.lang.Integer]) = {
    jsonMapper.writeValueAsBytes(new util.HashMap[TaskName, java.lang.Integer](mapping))
  }
}

object CheckpointSerde {
  val PARTITION_MAPPING_TYPEREFERENCE = new TypeReference[util.HashMap[TaskName, java.lang.Integer]]() {}
}
