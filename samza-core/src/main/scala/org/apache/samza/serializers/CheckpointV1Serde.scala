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
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

/**
 * Write out the Checkpoint object in JSON.  The underlying map of SSP => Offset cannot be stored directly because
 * JSON only allows strings as map types, so we would need to separately serialize the SSP to a string that doesn't
 * then interfere with JSON's decoding of the overall map.  We'll sidestep the whole issue by turning the
 * map into a list[String] of (System, Stream, Partition, Offset) serializing that.
 */
class CheckpointV1Serde extends Serde[CheckpointV1] with Logging {
  val jsonMapper = new ObjectMapper()

  // Serialize checkpoint as maps keyed by the SSP.toString() to the another map of the constituent SSP components
  // and offset. Jackson can't automatically serialize the SSP since it's not a POJO and this avoids
  // having to wrap it another class while maintaining readability.
  // { "SSP.toString()" -> {"system": system, "stream": stream, "partition": partition, "offset": offset)}
  def fromBytes(bytes: Array[Byte]): CheckpointV1 = {
    try {
      val jMap = jsonMapper.readValue(bytes, classOf[util.HashMap[String, util.HashMap[String, String]]])

      def deserializeJSONMap(sspInfo:util.HashMap[String, String]) = {
        require(sspInfo.size() == 4, "All JSON-encoded SystemStreamPartitions must have four keys")
        val system = sspInfo.get("system")
        require(system != null, "System must be present in JSON-encoded SystemStreamPartition")
        val stream = sspInfo.get("stream")
        require(stream != null, "Stream must be present in JSON-encoded SystemStreamPartition")
        val partition = sspInfo.get("partition")
        require(partition != null, "Partition must be present in JSON-encoded SystemStreamPartition")
        val offset = sspInfo.get("offset")
        // allow null offsets, e.g. for changelog ssps

        new SystemStreamPartition(system, stream, new Partition(partition.toInt)) -> offset
      }

      val cpMap = jMap.values.asScala.map(deserializeJSONMap).toMap

      new CheckpointV1(cpMap.asJava)
    } catch {
      case e : Exception =>
        warn("Exception while deserializing checkpoint: {}", util.Arrays.toString(bytes), e)
        null
    }
  }

  def toBytes(checkpoint: CheckpointV1): Array[Byte] = {
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