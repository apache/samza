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
  val IS_BOUNDED =            SAMZA_PROPERTY + "bounded"

  // We don't want any external dependencies on these patterns while both exist. Use getProperty to ensure proper values.
  private val STREAMS_PREFIX = "streams."
  private val STREAM_PREFIX = "systems.%s.streams.%s."

  val STREAM_ID_PREFIX = STREAMS_PREFIX + "%s."
  val SYSTEM_FOR_STREAM_ID = STREAM_ID_PREFIX + SYSTEM
  val PHYSICAL_NAME_FOR_STREAM_ID = STREAM_ID_PREFIX + PHYSICAL_NAME
  val IS_INTERMEDIATE_FOR_STREAM_ID = STREAM_ID_PREFIX + IS_INTERMEDIATE
  val IS_BOUNDED_FOR_STREAM_ID = STREAM_ID_PREFIX + IS_BOUNDED

  implicit def Config2Stream(config: Config) = new StreamConfig(config)
}

class StreamConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getStreamMsgSerde(systemStream: SystemStream) = nonEmptyOption(getSamzaProperty(systemStream, StreamConfig.MSG_SERDE))

  def getStreamKeySerde(systemStream: SystemStream) = nonEmptyOption(getSamzaProperty(systemStream, StreamConfig.KEY_SERDE))

  def getResetOffset(systemStream: SystemStream) =
    Option(getSamzaProperty(systemStream, StreamConfig.CONSUMER_RESET_OFFSET)) match {
      case Some("true") => true
      case Some("false") => false
      case Some(resetOffset) =>
        warn("Got a .samza.reset.offset configuration for SystemStream %s that is not true, or false (was %s). Defaulting to false."
          format (systemStream.toString format (systemStream.getSystem, systemStream.getStream), resetOffset))
        false
      case _ => false
    }

  def isResetOffsetConfigured(systemStream: SystemStream) = containsSamzaProperty(systemStream, StreamConfig.CONSUMER_RESET_OFFSET)

  def getDefaultStreamOffset(systemStream: SystemStream) =
    Option(getSamzaProperty(systemStream, StreamConfig.CONSUMER_OFFSET_DEFAULT))

  def isDefaultStreamOffsetConfigured(systemStream: SystemStream) = containsSamzaProperty(systemStream, StreamConfig.CONSUMER_OFFSET_DEFAULT)

  def getBootstrapEnabled(systemStream: SystemStream) =
    java.lang.Boolean.parseBoolean(getSamzaProperty(systemStream, StreamConfig.BOOTSTRAP))

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
    * Gets the properties for the specified streamId from the config.
    * It first applies any legacy configs from this config location:
    * systems.{system}.streams.{stream}.*
    *
    * It then overrides them with properties of the new config format:
    * streams.{streamId}.*
    *
    * Only returns properties of the stream itself, not any of the samza properties for the stream.
    *
    * @param streamId the identifier for the stream in the config.
    * @return         the merged map of config properties from both the legacy and new config styles
    */
  def getStreamProperties(streamId: String) = {
    val allProperties = getAllStreamProperties(streamId)
    val samzaProperties = allProperties.subset(StreamConfig.SAMZA_PROPERTY, false)
    val filteredStreamProperties:java.util.Map[String, String] = allProperties.asScala.filterKeys(k => !samzaProperties.containsKey(k)).asJava
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
    // use streamId as the default physical name
    get(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID format streamId, streamId)
  }

  /**
   * Gets the boolean flag of whether the specified streamId is an intermediate stream
   * @param streamId  the identifier for the stream in the config.
   * @return          true if the stream is intermediate
   */
  def getIsIntermediate(streamId: String) = {
    getBoolean(StreamConfig.IS_INTERMEDIATE_FOR_STREAM_ID format streamId, false)
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
    * Note, because the translation is not perfect between SystemStream and streamId,
    * this method is not identical to getProperty(streamId, property)
    *
    * @param systemStream the SystemStream for which the property value will be retrieved.
    * @param property the samza property name excluding the leading delimiter. e.g. "samza.x.y"
    */
  protected def getSamzaProperty(systemStream: SystemStream, property: String): String = {
    if (!property.startsWith(StreamConfig.SAMZA_PROPERTY)) {
      throw new IllegalArgumentException("Attempt to fetch a non samza property for SystemStream %s named %s" format(systemStream, property))
    }

    val streamVal = getAllStreamProperties(systemStreamToStreamId(systemStream)).get(property)
    if (streamVal != null) {
      streamVal
    } else {
      getSystemStreamProperties(systemStream.getSystem(), systemStream.getStream).get(property)
    }
  }

  /**
    * Gets the specified Samza property for a SystemStream. A Samza property is a property that controls how Samza
    * interacts with the stream, as opposed to a property of the stream itself.
    *
    * Note, because the translation is not perfect between SystemStream and streamId,
    * this method is not identical to getProperty(streamId, property)
    *
    * @param systemStream the SystemStream for which the property value will be retrieved.
    * @param property the samza property name excluding the leading delimiter. e.g. "samza.x.y"
    * @param defaultValue the default value to use if the property value is not found
    *
    */
  protected def getSamzaProperty(systemStream: SystemStream, property: String, defaultValue: String): String = {
    val streamVal = getSamzaProperty(systemStream, property)

    if (streamVal != null) {
      streamVal
    } else {
      defaultValue
    }
  }

  /**
    * Gets the specified Samza property for a SystemStream. A Samza property is a property that controls how Samza
    * interacts with the stream, as opposed to a property of the stream itself.
    *
    * Note, because the translation is not perfect between SystemStream and streamId,
    * this method is not identical to getProperty(streamId, property)
    */
  protected def containsSamzaProperty(systemStream: SystemStream, property: String): Boolean = {
    if (!property.startsWith(StreamConfig.SAMZA_PROPERTY)) {
      throw new IllegalArgumentException("Attempt to fetch a non samza property for SystemStream %s named %s" format(systemStream, property))
    }
    return getSamzaProperty(systemStream, property) != null
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
    if (systemName == null) {
      Map()
    }
    val systemConfig = new JavaSystemConfig(config);
    val defaults = systemConfig.getDefaultStreamProperties(systemName);
    val explicitConfigs = config.subset(StreamConfig.STREAM_PREFIX format(systemName, streamName), true)
    new MapConfig(defaults, explicitConfigs)
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
  private def getAllStreamProperties(streamId: String) = {
    val allProperties = subset(StreamConfig.STREAM_ID_PREFIX format streamId)
    val inheritedLegacyProperties:java.util.Map[String, String] = getSystemStreamProperties(getSystem(streamId), getPhysicalName(streamId))
    new MapConfig(java.util.Arrays.asList(inheritedLegacyProperties, allProperties))
  }

  private def getStreamIdsForSystem(system: String): Iterable[String] = {
    getStreamIds().filter(streamId => system.equals(getSystem(streamId)))
  }

  def systemStreamToStreamId(systemStream: SystemStream): String = {
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
