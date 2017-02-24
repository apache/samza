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

import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.system.SystemStream
import org.apache.samza.util.Logging

import scala.collection.JavaConversions._

object StreamConfig {
  // Samza configs for streams
  val SAMZA_PROPERTY = "samza."
  val SYSTEM =                  SAMZA_PROPERTY + "system"
  val PHYSICAL_NAME =           SAMZA_PROPERTY + "physical.name"
  val MSG_SERDE =               SAMZA_PROPERTY + "msg.serde"
  val KEY_SERDE =               SAMZA_PROPERTY + "key.serde"
  val CONSUMER_RESET_OFFSET =   SAMZA_PROPERTY + "reset.offset"
  val CONSUMER_OFFSET_DEFAULT = SAMZA_PROPERTY + "offset.default"

  val STREAM_PREFIX_BY_ID = "streams.%s."
  val SYSTEM_FOR_STREAM_ID = STREAM_PREFIX_BY_ID + SYSTEM
  val PHYSICAL_NAME_FOR_STREAM_ID = STREAM_PREFIX_BY_ID + PHYSICAL_NAME
  val SAMZA_STREAM_PROPERTY_PREFIX = STREAM_PREFIX_BY_ID + SAMZA_PROPERTY

  val STREAM_PREFIX = "systems.%s.streams.%s."
  val SYSTEM_STREAM_MSG_SERDE = STREAM_PREFIX + MSG_SERDE
  val SYSTEM_STREAM_KEY_SERDE = STREAM_PREFIX + KEY_SERDE
  val SYSTEM_STREAM_CONSUMER_RESET_OFFSET = STREAM_PREFIX + CONSUMER_RESET_OFFSET
  val SYSTEM_STREAM_CONSUMER_OFFSET_DEFAULT = STREAM_PREFIX + CONSUMER_OFFSET_DEFAULT

  implicit def Config2Stream(config: Config) = new StreamConfig(config)
}

class StreamConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getStreamMsgSerde(systemStream: SystemStream) =
    getNonEmptyOption(StreamConfig.SYSTEM_STREAM_MSG_SERDE format (systemStream.getSystem, systemStream.getStream))

  def getStreamKeySerde(systemStream: SystemStream) =
    getNonEmptyOption(StreamConfig.SYSTEM_STREAM_KEY_SERDE format (systemStream.getSystem, systemStream.getStream))

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
    getOption(StreamConfig.SYSTEM_STREAM_CONSUMER_RESET_OFFSET format (systemStream.getSystem, systemStream.getStream)) match {
      case Some("true") => true
      case Some("false") => false
      case Some(resetOffset) =>
        warn("Got a configuration for %s that is not true, or false (was %s). Defaulting to false." format (StreamConfig.SYSTEM_STREAM_CONSUMER_RESET_OFFSET format (systemStream.getSystem, systemStream.getStream), resetOffset))
        false
      case _ => false
    }

  def getDefaultStreamOffset(systemStream: SystemStream) =
    getOption(StreamConfig.SYSTEM_STREAM_CONSUMER_OFFSET_DEFAULT format (systemStream.getSystem, systemStream.getStream))

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

  /**
    * Gets the stream properties from the legacy config style:
    * systems.{system}.streams.{streams}.*
    *
    * @param systemName the system name under which the properties are configured
    * @param streamName the stream name
    * @return           the map of properties for the stream
    */
  def getSystemStreamProperties(systemName: String, streamName: String) = {
    config.subset(StreamConfig.STREAM_PREFIX format(systemName, streamName), true)
  }

  /**
    * Gets the properties for the specified streamId from the config.
    * This method supercedes {@link StreamConfig.#getSystemStreamProperties}
    * It first applies any legacy configs from this config location:
    * systems.{system}.streams.{stream}.*
    *
    * It then overrides them with properties of the new config format:
    * streams.{streamId}.*
    *
    * @param streamId the identifier for the stream in the config.
    * @return         the merged map of config properties from both the legacy and new config styles
    */
  def getStreamProperties(streamId: String) = {
    val allProperties = subset(StreamConfig.STREAM_PREFIX_BY_ID format streamId)
    val samzaProperties = allProperties.subset(StreamConfig.SAMZA_PROPERTY, false)
    val filteredStreamProperties:java.util.Map[String, String] = allProperties.filterKeys(k => !samzaProperties.containsKey(k))
    val inheritedLegacyProperties:java.util.Map[String, String] = getSystemStreamProperties(getSystem(streamId), streamId)
    new MapConfig(java.util.Arrays.asList(inheritedLegacyProperties, filteredStreamProperties))
  }

  /**
    * Gets the System associated with the specified streamId.
    * It first looks for the property
    * streams.{streamId}.system
    *
    * If no value was provided, it uses
    * job.default.system
    *
    * @param streamId the identifier for the stream in the config.
    * @return         the system name associated with the stream.
    */
  def getSystem(streamId: String) = {
    getOption(StreamConfig.SYSTEM_FOR_STREAM_ID format streamId) match {
      case Some(system) => system
      case _ => config.getDefaultSystem.getOrElse(throw new ConfigException("Missing %s configuration. Cannot bind stream to a system without it."
        format(StreamConfig.SYSTEM_FOR_STREAM_ID format(streamId))))
    }
  }

  /**
    * Gets the physical name for the specified streamId.
    *
    * @param streamId             the identifier for the stream in the config.
    * @param defaultPhysicalName  the default to use if the physical name is missing.
    * @return                     the physical identifier for the stream or the default if it is undefined.
    */
  def getPhysicalName(streamId: String, defaultPhysicalName: String) = {
    get(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID format streamId, defaultPhysicalName)
  }
}
