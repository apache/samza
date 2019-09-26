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

import org.apache.samza.system.SystemStream
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

object StreamConfig {
  // Samza configs for streams
  val SAMZA_PROPERTY = "samza."
  val SYSTEM =                  SAMZA_PROPERTY + "system"
  val PHYSICAL_NAME =           SAMZA_PROPERTY + "physical.name"
  val MSG_SERDE =               SAMZA_PROPERTY + "msg.serde"
  val KEY_SERDE =               SAMZA_PROPERTY + "key.serde"
  val CONSUMER_RESET_OFFSET =   SAMZA_PROPERTY + "reset.offset"
  val CONSUMER_OFFSET_DEFAULT = SAMZA_PROPERTY + "offset.default"
  val BOOTSTRAP =               SAMZA_PROPERTY + "bootstrap"
  val PRIORITY =                SAMZA_PROPERTY + "priority"
  val IS_INTERMEDIATE =         SAMZA_PROPERTY + "intermediate"
  val DELETE_COMMITTED_MESSAGES = SAMZA_PROPERTY + "delete.committed.messages"
  val IS_BOUNDED =              SAMZA_PROPERTY + "bounded"
  val BROADCAST =            SAMZA_PROPERTY + "broadcast"

  // We don't want any external dependencies on these patterns while both exist. Use getProperty to ensure proper values.
  private val STREAMS_PREFIX = "streams."

  val STREAM_PREFIX = "systems.%s.streams.%s."
  val STREAM_ID_PREFIX = STREAMS_PREFIX + "%s."
  val SYSTEM_FOR_STREAM_ID = STREAM_ID_PREFIX + SYSTEM
  val PHYSICAL_NAME_FOR_STREAM_ID = STREAM_ID_PREFIX + PHYSICAL_NAME
  val IS_INTERMEDIATE_FOR_STREAM_ID = STREAM_ID_PREFIX + IS_INTERMEDIATE
  val DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID = STREAM_ID_PREFIX + DELETE_COMMITTED_MESSAGES
  val IS_BOUNDED_FOR_STREAM_ID = STREAM_ID_PREFIX + IS_BOUNDED
  val PRIORITY_FOR_STREAM_ID = STREAM_ID_PREFIX + PRIORITY
  val CONSUMER_OFFSET_DEFAULT_FOR_STREAM_ID = STREAM_ID_PREFIX + CONSUMER_OFFSET_DEFAULT
  val BOOTSTRAP_FOR_STREAM_ID = STREAM_ID_PREFIX + BOOTSTRAP
  val BROADCAST_FOR_STREAM_ID = STREAM_ID_PREFIX + BROADCAST

  implicit def Config2Stream(config: Config) = new StreamConfig(config)
}

/**
 * Helper for accessing configs related to stream properties.
 *
 * For most configs, this currently supports two different formats for specifying stream properties:
 * 1) "streams.{streamId}.{property}" (recommended to use this format)
 * 2) "systems.{systemName}.streams.{streamName}.{property}" (legacy)
 * Note that some config lookups are only supported through the "streams.{streamId}.{property}". See the specific
 * accessor method to determine which formats are supported.
 *
 * Summary of terms:
 * - streamId: logical identifier used for a stream; configs are specified using this streamId
 * - physical stream: concrete name for a stream (if the physical stream is not explicitly configured, then the streamId
 *   is used as the physical stream
 * - streamName: within the javadoc for this class, streamName is the same as physical stream
 * - samza property: property which is Samza-specific, which will have "samza." as a prefix (e.g. "samza.key.serde");
 *   this is in contrast to stream-specific properties which are related to specific stream technologies
 */
class StreamConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getStreamMsgSerde(systemStream: SystemStream) = nonEmptyOption(getSamzaProperty(systemStream, StreamConfig.MSG_SERDE))

  def getStreamKeySerde(systemStream: SystemStream) = nonEmptyOption(getSamzaProperty(systemStream, StreamConfig.KEY_SERDE))

  def getResetOffset(systemStream: SystemStream) =
    Option(getSamzaProperty(systemStream, StreamConfig.CONSUMER_RESET_OFFSET)) match {
      case Some("true") => true
      case Some("false") => false
      case Some(resetOffset) =>
        warn("Got a .samza.reset.offset configuration for SystemStream %s that is not true or false (was %s). Defaulting to false."
          format (systemStream.toString format (systemStream.getSystem, systemStream.getStream), resetOffset))
        false
      case _ => false
    }

  def isResetOffsetConfigured(systemStream: SystemStream) =
    containsSamzaProperty(systemStream, StreamConfig.CONSUMER_RESET_OFFSET)

  def getDefaultStreamOffset(systemStream: SystemStream) =
    Option(getSamzaProperty(systemStream, StreamConfig.CONSUMER_OFFSET_DEFAULT))

  def isDefaultStreamOffsetConfigured(systemStream: SystemStream) =
    containsSamzaProperty(systemStream, StreamConfig.CONSUMER_OFFSET_DEFAULT)

  def getBootstrapEnabled(systemStream: SystemStream) =
    java.lang.Boolean.parseBoolean(getSamzaProperty(systemStream, StreamConfig.BOOTSTRAP))

  def getBroadcastEnabled(systemStream: SystemStream) =
    java.lang.Boolean.parseBoolean(getSamzaProperty(systemStream, StreamConfig.BROADCAST))

  def getPriority(systemStream: SystemStream) =
    java.lang.Integer.parseInt(getSamzaProperty(systemStream, StreamConfig.PRIORITY, "-1"))

  /**
   * Returns a list of all SystemStreams that have a serde defined from the config file.
   */
  def getSerdeStreams(systemName: String) = {
    val subConf = config.subset("systems.%s.streams." format systemName, true)
    val legacySystemStreams = subConf
      .asScala
      .keys
      .filter(k => k.endsWith(StreamConfig.MSG_SERDE) || k.endsWith(StreamConfig.KEY_SERDE))
      .map(k => {
        val streamName = k.substring(0, k.length - 16 /* .samza.XXX.serde length */ )
        new SystemStream(systemName, streamName)
      }).toSet

    val systemStreams = subset(StreamConfig.STREAMS_PREFIX)
      .asScala
      .keys
      .filter(k => k.endsWith(StreamConfig.MSG_SERDE) || k.endsWith(StreamConfig.KEY_SERDE))
      .map(k => k.substring(0, k.length - 16 /* .samza.XXX.serde length */ ))
      .filter(streamId => systemName.equals(getSystem(streamId)))
      .map(streamId => streamIdToSystemStream(streamId)).toSet

    legacySystemStreams.union(systemStreams)
  }

  /**
   * Gets the properties for the streamId which are not Samza properties (i.e. do not have a "samza." prefix). This
   * includes current and legacy config styles.
   * This will return a Config with the properties that match the following formats (if a property is specified through
   * multiple formats, priority is top to bottom):
   * 1) "streams.{streamId}.{property}"
   * 2) "systems.{systemName}.streams.{streamName}.{property}" where systemName is the system mapped to the streamId in
   * the config and streamName is the physical stream name mapped to the stream id
   * 3) "systems.{systemName}.default.stream.{property}" where systemName is the system mapped to the streamId in the
   * config
   *
   * @param streamId the identifier for the stream in the config.
   * @return         the merged map of config properties from both the legacy and new config styles
   */
  def getStreamProperties(streamId: String) = {
    val allProperties = getAllStreamProperties(streamId)
    val samzaProperties = allProperties.subset(StreamConfig.SAMZA_PROPERTY, false)
    val filteredStreamProperties: java.util.Map[String, String] =
      allProperties.asScala.filterKeys(k => !samzaProperties.containsKey(k)).asJava
    new MapConfig(filteredStreamProperties)
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
      case _ => new JobConfig(config).getDefaultSystem().orElse(null)
    }
  }

  /**
    * Gets the physical name for the specified streamId.
    *
    * @param streamId             the identifier for the stream in the config.
    * @return                     the physical identifier for the stream or the default if it is undefined.
    */
  def getPhysicalName(streamId: String) = {
    // use streamId as the default physical name
    get(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID format streamId, streamId)
  }

  /**
   * Gets the boolean flag of whether the specified streamId is an intermediate stream
   * @param streamId  the identifier for the stream in the config.
   * @return          true if the stream is intermediate
   */
  def getIsIntermediateStream(streamId: String) = {
    getBoolean(StreamConfig.IS_INTERMEDIATE_FOR_STREAM_ID format streamId, false)
  }

  /**
    * Gets the boolean flag of whether the committed messages specified streamId can be deleted
    * @param streamId  the identifier for the stream in the config.
    * @return          true if the committed messages of the stream can be deleted
    */
  def getDeleteCommittedMessages(streamId: String) = {
    getBoolean(StreamConfig.DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID format streamId, false)
  }

  def getIsBounded(streamId: String) = {
    getBoolean(StreamConfig.IS_BOUNDED_FOR_STREAM_ID format streamId, false)
  }

  /**
   * Gets the stream IDs of all the streams defined in the config
   * @return collection of stream IDs
   */
  def getStreamIds(): Iterable[String] = {
    // StreamIds are not allowed to have '.' so the first index of '.' marks the end of the streamId.
    subset(StreamConfig.STREAMS_PREFIX).asScala.keys.map(key => key.substring(0, key.indexOf(".")))
  }

  /**
   * Gets the specified Samza property for a SystemStream. A Samza property is a property that controls how Samza
   * interacts with the stream, as opposed to a property of the stream itself.
   *
   * First, tries to map the systemStream to a streamId. This will only find a streamId if the stream is a physical name
   * (explicitly mapped physical name or a stream id without a physical name mapping). That means this will not map a
   * stream id to itself if there is a mapping from the stream id to a physical stream name. This also requires that the
   * stream id is mapped to a system in the config.
   * If a stream id is found:
   * 1) Look for "streams.{streamId}.{property}" for the stream id.
   * 2) Otherwise, look for "systems.{systemName}.streams.{streamName}.{property}" in which the systemName is the system
   * mapped to the stream id and the streamName is the physical stream name for the stream id.
   * 3) Otherwise, look for "systems.{systemName}.default.stream.{property}" in which the systemName is the system
   * mapped to the stream id.
   * If a stream id was not found or no property could be found using the above keys:
   * 1) Look for "systems.{systemName}.streams.{streamName}.{property}" in which the systemName is the system in the
   * input systemStream and the streamName is the stream from the input systemStream.
   * 2) Otherwise, look for "systems.{systemName}.default.stream.{property}" in which the systemName is the system
   * in the input systemStream.
   *
   * @param systemStream the SystemStream for which the property value will be retrieved.
   * @param property the samza property key (including the "samza." prefix); for example, for both
   *                 "streams.streamId.samza.prop.key" and "systems.system.streams.streamName.samza.prop.key", this
   *                 argument should have the value "samza.prop.key"
   */
  private def getSamzaProperty(systemStream: SystemStream, property: String): String = {
    if (!property.startsWith(StreamConfig.SAMZA_PROPERTY)) {
      throw new IllegalArgumentException(
        "Attempt to fetch a non samza property for SystemStream %s named %s" format(systemStream, property))
    }

    val streamVal = getAllStreamProperties(systemStreamToStreamId(systemStream)).get(property)
    if (streamVal != null) {
      streamVal
    } else {
      getSystemStreamProperties(systemStream.getSystem(), systemStream.getStream).get(property)
    }
  }

  /**
   * Gets a Samza property, with a default value used if no property value is found.
   * See getSamzaProperty(SystemStream, String).
   *
   * @param systemStream the SystemStream for which the property value will be retrieved.
   * @param property the samza property key (including the "samza." prefix); for example, for both
   *                 "streams.streamId.samza.prop.key" and "systems.system.streams.streamName.samza.prop.key", this
   *                 argument should have the value "samza.prop.key"
   * @param defaultValue the default value to use if the property value is not found
   */
  private def getSamzaProperty(systemStream: SystemStream, property: String, defaultValue: String): String = {
    val streamVal = getSamzaProperty(systemStream, property)

    if (streamVal != null) {
      streamVal
    } else {
      defaultValue
    }
  }

  /**
   * Determines if a Samza property is specified.
   * See getSamzaProperty(SystemStream, String).
   *
   * @param systemStream the SystemStream for the property value to check
   * @param property the samza property key (including the "samza." prefix); for example, for both
   *                 "streams.streamId.samza.prop.key" and "systems.system.streams.streamName.samza.prop.key", this
   *                 argument should have the value "samza.prop.key"
   */
  private def containsSamzaProperty(systemStream: SystemStream, property: String): Boolean = {
    if (!property.startsWith(StreamConfig.SAMZA_PROPERTY)) {
      throw new IllegalArgumentException(
        "Attempt to fetch a non samza property for SystemStream %s named %s" format(systemStream, property))
    }
    getSamzaProperty(systemStream, property) != null
  }


  /**
   * Finds the properties from the legacy config style (config key includes system).
   * This will return a Config with the properties that match the following formats (if a property is specified through
   * multiple formats, priority is top to bottom):
   * 1) "systems.{systemName}.streams.{streamName}.{property}"
   * 2) "systems.{systemName}.default.stream.{property}"
   *
   * @param systemName the system name under which the properties are configured
   * @param streamName the stream name
   * @return           the map of properties for the stream
   */
  private def getSystemStreamProperties(systemName: String, streamName: String) = {
    if (systemName == null) {
      Map()
    }
    val systemConfig = new SystemConfig(config)
    val defaults = systemConfig.getDefaultStreamProperties(systemName)
    val explicitConfigs = config.subset(StreamConfig.STREAM_PREFIX format(systemName, streamName), true)
    new MapConfig(defaults, explicitConfigs)
  }

  /**
   * Gets all of the properties for the specified streamId (includes current and legacy config styles).
   * This will return a Config with the properties that match the following formats (if a property is specified through
   * multiple formats, priority is top to bottom):
   * 1) "streams.{streamId}.{property}"
   * 2) "systems.{systemName}.streams.{streamName}.{property}" where systemName is the system mapped to the streamId in
   * the config and streamName is the physical stream name mapped to the stream id
   * 3) "systems.{systemName}.default.stream.{property}" where systemName is the system mapped to the streamId in the
   * config
   *
   * @param streamId the identifier for the stream in the config.
   * @return         the merged map of config properties from both the legacy and new config styles
   */
  private def getAllStreamProperties(streamId: String) = {
    val allProperties = subset(StreamConfig.STREAM_ID_PREFIX format streamId)
    val inheritedLegacyProperties: java.util.Map[String, String] =
      getSystemStreamProperties(getSystem(streamId), getPhysicalName(streamId))
    new MapConfig(java.util.Arrays.asList(inheritedLegacyProperties, allProperties))
  }

  private def getStreamIdsForSystem(system: String): Iterable[String] = {
    getStreamIds().filter(streamId => system.equals(getSystem(streamId)))
  }

  /**
   * Finds the stream id which corresponds to the systemStream.
   * This finds the stream id that is mapped to the system in systemStream through the config and that has a physical
   * name (the physical name might be the stream id itself if there is no explicit mapping) that matches the stream in
   * systemStream.
   * Note: If the stream in the systemStream is a stream id which is mapped to a physical stream, then that stream won't
   * be returned as a stream id here, since the stream in systemStream doesn't match the physical stream name.
   *
   * @param systemStream system stream to map to stream id
   * @return             stream id corresponding to the system stream
   */
  private def systemStreamToStreamId(systemStream: SystemStream): String = {
    val streamIds = getStreamIdsForSystem(systemStream.getSystem)
      .filter(streamId => systemStream.getStream().equals(getPhysicalName(streamId)))
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
  def streamIdToSystemStream(streamId: String): SystemStream = {
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
