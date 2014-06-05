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

import scala.collection.JavaConversions._
import org.codehaus.jackson.map.ObjectMapper
import org.apache.samza.system.SystemStream
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.SamzaException
import grizzled.slf4j.Logging

class CheckpointSerde extends Serde[Checkpoint] with Logging {
  val jsonMapper = new ObjectMapper()

  def fromBytes(bytes: Array[Byte]): Checkpoint = {
    try {
      val checkpointMap = jsonMapper
        .readValue(bytes, classOf[java.util.Map[String, java.util.Map[String, String]]])
        .flatMap {
          case (systemName, streamToOffsetMap) =>
            streamToOffsetMap.map { case (streamName, offset) => (new SystemStream(systemName, streamName), offset) }
        }
      return new Checkpoint(checkpointMap)
    } catch {
      case e : Exception =>
        warn("Exception while deserializing checkpoint: " + e)
        debug("Exception detail:", e)
        null
    }
  }

  def toBytes(checkpoint: Checkpoint) = {
    val offsetMap = mapAsJavaMap(checkpoint
      .getOffsets
      // Convert Map[SystemStream, String] offset map to a iterable of tuples (system, stream, offset)
      .map { case (systemStream, offset) => (systemStream.getSystem, systemStream.getStream, offset) }
      // Group into a Map[String, (String, String, String)] by system
      .groupBy(_._1)
      // Group the tuples for each system into a Map[String, String] for stream to offsets
      .map {
        case (systemName, tuples) =>
          val streamToOffestMap = mapAsJavaMap(tuples
            // Group the tuples by stream name
            .groupBy(_._2)
            // There should only ever be one SystemStream to offset mapping, so just 
            // grab the first element from the tuple list for each stream.
            .map {
              case (streamName, tuples) => {
                // If there's more than one offset, something is seriously wrong.
                if (tuples.size != 1) {
                  throw new SamzaException("Got %s offsets for %s. Expected only one offset, so failing." format (tuples.size, streamName))
                }
                (streamName, tuples.head._3)
              }
            }
            .toMap)
          (systemName, streamToOffestMap)
      }.toMap)

    jsonMapper.writeValueAsBytes(offsetMap)
  }
}
