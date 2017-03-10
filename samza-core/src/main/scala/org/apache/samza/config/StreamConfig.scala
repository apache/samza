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

  val STREAMS_PREFIX = "streams."
  val STREAM_ID_PREFIX = STREAMS_PREFIX + "%s."
  val SYSTEM_FOR_STREAM_ID = STREAM_ID_PREFIX + SYSTEM
  val PHYSICAL_NAME_FOR_STREAM_ID = STREAM_ID_PREFIX + PHYSICAL_NAME
  val SAMZA_STREAM_PROPERTY_PREFIX = STREAM_ID_PREFIX + SAMZA_PROPERTY

  val STREAM_PREFIX = "systems.%s.streams.%s."

  implicit def Config2Stream(config: Config) = new StreamConfig(config)
}

class StreamConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getStreamMsgSerde(systemStream: SystemStream) = nonEmptyOption(getProperty(systemStream, StreamConfig.MSG_SERDE))

  def getStreamKeySerde(systemStream: SystemStream) = nonEmptyOption(getProperty(systemStream, StreamConfig.KEY_SERDE))

  def getResetOffset(systemStream: SystemStream) =
    Option(getProperty(systemStream, StreamConfig.CONSUMER_RESET_OFFSET)) match {
      case Some("true") => true
      case Some("false") => false
      case Some(resetOffset) =>
        warn("Got a .samza.reset.offset configuration for SystemStream %s that is not true, or false (was %s). Defaulting to false."
          format (systemStream.toString format (systemStream.getSystem, systemStream.getStream), resetOffset))
        false
      case _ => false
    }

  def getDefaultStreamOffset(systemStream: SystemStream) =
    Option(getProperty(systemStream, StreamConfig.CONSUMER_OFFSET_DEFAULT))

  /**
   * Returns a list of all SystemStreams that have a serde defined from the config file.
   */
  def getSerdeStreams(systemName: String) = {
    val subConf = config.subset("systems.%s.streams." format systemName, true)
    val legacySystemStreams = subConf
      .keys
      .filter(k => k.endsWith(StreamConfig.MSG_SERDE) || k.endsWith(StreamConfig.KEY_SERDE))
      .map(k => {
        val streamName = k.substring(0, k.length - 16 /* .samza.XXX.serde length */ )
        new SystemStream(systemName, streamName)
      }).toSet

    val systemStreams = subset(StreamConfig.STREAMS_PREFIX)
      .keys
      .filter(k => k.endsWith(StreamConfig.MSG_SERDE) || k.endsWith(StreamConfig.KEY_SERDE))
      .map(k => {
        val streamId = k.substring(0, k.length - 16 /* .samza.XXX.serde length */ )
        streamIdToSystemStream(streamId)
      }).toSet

    legacySystemStreams.union(systemStreams)
  }

  /**
    * Gets the stream properties from the legacy config style:
    * systems.{system}.streams.{streams}.*
    *
    * @param systemName the system name under which the properties are configured
    * @param streamName the stream name
    * @return           the map of properties for the stream
    */
  private def getSystemStreamProperties(systemName: String, streamName: String) = {
    if (systemName == null || streamName == null) {
      Map()
    }
    config.subset(StreamConfig.STREAM_PREFIX format(systemName, streamName), true)
  }

  /**
    * Gets the properties for the specified streamId from the config.
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
    val allProperties = subset(StreamConfig.STREAM_ID_PREFIX format streamId)
    val samzaProperties = allProperties.subset(StreamConfig.SAMZA_PROPERTY, false)
    val filteredStreamProperties:java.util.Map[String, String] = allProperties.filterKeys(k => !samzaProperties.containsKey(k))
    val inheritedLegacyProperties:java.util.Map[String, String] = getSystemStreamProperties(getSystem(streamId), getPhysicalName(streamId))
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
      case _ => config.getDefaultSystem.orNull
    }
  }

  /**
    * Gets the physical name for the specified streamId.
    *
    * @param streamId             the identifier for the stream in the config.
    * @return                     the physical identifier for the stream or the default if it is undefined.
    */
  def getPhysicalName(streamId: String) = {
    // Add prefix to default so the physical name is unique per job
    val defaultPhysicalName = String.format("%s-%s-%s",
                                            config.get(JobConfig.JOB_NAME),
                                            config.get(JobConfig.JOB_ID, "1"),
                                            streamId)
    get(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID format streamId, defaultPhysicalName)
  }

  /**
    * Gets the specified property for a SystemStream.
    *
    * Note, because the translation is not perfect between SystemStream and streamId,
    * this method is not identical to getProperty(streamId, property)
    */
  private def getProperty(systemStream: SystemStream, property: String): String = {
    val streamVal = getStreamProperties(systemStreamToStreamId(systemStream)).get(property)

    if (streamVal != null) {
      streamVal
    } else {
      getSystemStreamProperties(systemStream.getSystem(), systemStream.getStream).get(property)
    }
  }

  private def getStreamIds(): Iterable[String] = {
    subset(StreamConfig.STREAMS_PREFIX).keys
  }

  private def getStreamIdsForSystem(system: String): Iterable[String] = {
    getStreamIds().filter(streamId => system.equals(getSystem(streamId)))
  }

  private def systemStreamToStreamId(systemStream: SystemStream): String = {
   val streamIds = getStreamIdsForSystem(systemStream.getSystem).filter(streamId => systemStream.getStream().equals(getPhysicalName(streamId)))
    if (streamIds.size > 1) {
      throw new IllegalStateException("There was more than one stream found for system stream %s" format(systemStream))
    }

    if (streamIds.isEmpty) {
      null
    } else {
      streamIds.head
    }
  }

  /**
    * A streamId is translated to a SystemStream by looking up its System and physicalName. It
    * will use the streamId as the stream name if the physicalName doesn't exist.
    */
  private def streamIdToSystemStream(streamId: String): SystemStream = {
    new SystemStream(getSystem(streamId), getPhysicalName(streamId))
  }

  private def nonEmptyOption(value: String): Option[String] = {
    if (value == null || value.isEmpty) {
      None
    } else {
      Some(value)
    }
  }
}
