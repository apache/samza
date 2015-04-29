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

package org.apache.samza.config

import org.apache.samza.util.Logging
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemStream

object StreamConfig {
  // stream config constants
  val STREAM_PREFIX = "systems.%s.streams.%s."
  val MSG_SERDE = STREAM_PREFIX + "samza.msg.serde"
  val KEY_SERDE = STREAM_PREFIX + "samza.key.serde"
  val CONSUMER_RESET_OFFSET = STREAM_PREFIX + "samza.reset.offset"
  val CONSUMER_OFFSET_DEFAULT = STREAM_PREFIX + "samza.offset.default"

  implicit def Config2Stream(config: Config) = new StreamConfig(config)
}

class StreamConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getStreamMsgSerde(systemStream: SystemStream) =
    getOption(StreamConfig.MSG_SERDE format (systemStream.getSystem, systemStream.getStream))

  def getStreamKeySerde(systemStream: SystemStream) =
    getOption(StreamConfig.KEY_SERDE format (systemStream.getSystem, systemStream.getStream))

  def getResetOffsetMap(systemName: String) = {
    val subConf = config.subset("systems.%s.streams." format systemName, true)
    subConf
      .filterKeys(k => k.endsWith(".samza.reset.offset"))
      .map(kv => {
        val streamName = kv._1.replace(".samza.reset.offset", "")
        val systemStream = new SystemStream(systemName, streamName)
        val resetVal = getResetOffset(systemStream)
        (systemStream, resetVal)
      }).toMap
  }

  def getResetOffset(systemStream: SystemStream) =
    getOption(StreamConfig.CONSUMER_RESET_OFFSET format (systemStream.getSystem, systemStream.getStream)) match {
      case Some("true") => true
      case Some("false") => false
      case Some(resetOffset) =>
        warn("Got a configuration for %s that is not true, or false (was %s). Defaulting to false." format (StreamConfig.CONSUMER_RESET_OFFSET format (systemStream.getSystem, systemStream.getStream), resetOffset))
        false
      case _ => false
    }

  def getDefaultStreamOffset(systemStream: SystemStream) =
    getOption(StreamConfig.CONSUMER_OFFSET_DEFAULT format (systemStream.getSystem, systemStream.getStream))

  /**
   * Returns a list of all SystemStreams that have a serde defined from the config file.
   */
  def getSerdeStreams(systemName: String) = {
    val subConf = config.subset("systems.%s.streams." format systemName, true)
    subConf
      .keys
      .filter(k => k.endsWith(".samza.msg.serde") || k.endsWith(".samza.key.serde"))
      .map(k => {
        val streamName = k.substring(0, k.length - 16 /* .samza.XXX.serde length */ )
        new SystemStream(systemName, streamName)
      }).toSet
  }
}
