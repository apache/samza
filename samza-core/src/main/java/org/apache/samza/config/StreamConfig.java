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

package org.apache.samza.config;

import com.google.common.collect.Sets;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamConfig extends MapConfig {
  public static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  // Samza configs for streams
  public static final String SAMZA_PROPERTY = "samza.";
  public static final String SYSTEM = SAMZA_PROPERTY + "system";
  public static final String PHYSICAL_NAME = SAMZA_PROPERTY + "physical.name";
  public static final String MSG_SERDE = SAMZA_PROPERTY + "msg.serde";
  public static final String KEY_SERDE = SAMZA_PROPERTY + "key.serde";
  public static final String CONSUMER_RESET_OFFSET = SAMZA_PROPERTY + "reset.offset";
  public static final String CONSUMER_OFFSET_DEFAULT = SAMZA_PROPERTY + "offset.default";
  public static final String BOOTSTRAP = SAMZA_PROPERTY + "bootstrap";
  public static final String PRIORITY = SAMZA_PROPERTY + "priority";
  public static final String IS_INTERMEDIATE = SAMZA_PROPERTY + "intermediate";
  public static final String DELETE_COMMITTED_MESSAGES = SAMZA_PROPERTY + "delete.committed.messages";
  public static final String IS_BOUNDED = SAMZA_PROPERTY + "bounded";
  public static final String BROADCAST = SAMZA_PROPERTY + "broadcast";

  // We don't want any external dependencies on these patterns while both exist.
  // Use the corresponding get*() method to ensure proper values.
  private static final String STREAMS_PREFIX = "streams.";

  public static final String STREAM_PREFIX = "systems.%s.streams.%s.";
  public static final String STREAM_ID_PREFIX = STREAMS_PREFIX + "%s.";
  public static final String SYSTEM_FOR_STREAM_ID = STREAM_ID_PREFIX + SYSTEM;
  public static final String PHYSICAL_NAME_FOR_STREAM_ID = STREAM_ID_PREFIX + PHYSICAL_NAME;
  public static final String IS_INTERMEDIATE_FOR_STREAM_ID = STREAM_ID_PREFIX + IS_INTERMEDIATE;
  public static final String DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID = STREAM_ID_PREFIX + DELETE_COMMITTED_MESSAGES;
  public static final String IS_BOUNDED_FOR_STREAM_ID = STREAM_ID_PREFIX + IS_BOUNDED;
  public static final String PRIORITY_FOR_STREAM_ID = STREAM_ID_PREFIX + PRIORITY;
  public static final String CONSUMER_OFFSET_DEFAULT_FOR_STREAM_ID = STREAM_ID_PREFIX + CONSUMER_OFFSET_DEFAULT;
  public static final String BOOTSTRAP_FOR_STREAM_ID = STREAM_ID_PREFIX + BOOTSTRAP;
  public static final String BROADCAST_FOR_STREAM_ID = STREAM_ID_PREFIX + BROADCAST;

  /*
   * Implementation notes:
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

  private Optional<String> nonEmptyOption(String value) {
    if (value == null || value.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(value);
    }
  }

  public StreamConfig(Config config) {
    super(config);
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
  private Map<String, String> getSystemStreamProperties(String systemName, String streamName) {
    if (systemName == null) {
      Collections.emptyMap();
    }
    SystemConfig systemConfig = new SystemConfig(this);
    Config defaults = systemConfig.getDefaultStreamProperties(systemName);
    Config explicitConfigs = subset(String.format(STREAM_PREFIX, systemName, streamName), true);
    return new MapConfig(defaults, explicitConfigs);
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
  private MapConfig getAllStreamProperties(String streamId) {
    Config allProperties = subset(String.format(STREAM_ID_PREFIX, streamId));
    Map<String, String> inheritedLegacyProperties = getSystemStreamProperties(getSystem(streamId), getPhysicalName(streamId));
    return new MapConfig(Arrays.asList(inheritedLegacyProperties, allProperties));
  }

  /**
   * Gets the distinct stream IDs of all the streams defined in the config
   *
   * @return collection of stream IDs
   */
  public Set<String> getStreamIds() {
    // StreamIds are not allowed to have '.' so the first index of '.' marks the end of the streamId.
    return subset(STREAMS_PREFIX).keySet().stream().map(key -> key.substring(0, key.indexOf(".")))
      .distinct().collect(Collectors.toSet());
  }

  private List<String> getStreamIdsForSystem(String system) {
    return getStreamIds().stream().filter(streamId -> system.equals(getSystem(streamId))).collect(Collectors.toList());
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
  private String systemStreamToStreamId(SystemStream systemStream) {
    List<String> streamIds = getStreamIdsForSystem(systemStream.getSystem()).stream()
      .filter(streamId -> systemStream.getStream().equals(getPhysicalName(streamId))).collect(Collectors.toList());
    if (streamIds.size() > 1) {
      throw new IllegalStateException(String.format("There was more than one stream found for system stream %s", systemStream));
    }

    return streamIds.isEmpty() ? null : streamIds.get(0);
  }

  /**
   * Gets the System associated with the specified streamId.
   * It first looks for the property
   * streams.{streamId}.system
   * <p>
   * If no value was provided, it uses
   * job.default.system
   *
   * @param streamId the identifier for the stream in the config.
   * @return the system name associated with the stream or null.
   */
  public String getSystem(String streamId) {
    String system = get(String.format(SYSTEM_FOR_STREAM_ID, streamId));
    return (system != null) ? system : new JobConfig(this).getDefaultSystem().orElse(null);
  }

  /**
   * Gets the physical name for the specified streamId.
   *
   * @param streamId the identifier for the stream in the config.
   * @return the physical identifier for the stream or the default if it is undefined.
   */
  public String getPhysicalName(String streamId) {
    // use streamId as the default physical name
    return getOrDefault(String.format(PHYSICAL_NAME_FOR_STREAM_ID, streamId), streamId);
  }


  public Optional<String> getStreamMsgSerde(SystemStream systemStream) {
    return nonEmptyOption(getSamzaProperty(systemStream, MSG_SERDE));
  }

  public Optional<String> getStreamKeySerde(SystemStream systemStream) {
    return nonEmptyOption(getSamzaProperty(systemStream, KEY_SERDE));
  }

  public boolean getResetOffset(SystemStream systemStream) {
    String resetOffset = getSamzaProperty(systemStream, CONSUMER_RESET_OFFSET, "false");
    if (!resetOffset.equalsIgnoreCase("true") && !resetOffset.equalsIgnoreCase("false")) {
      LOG.warn("Got a .samza.reset.offset configuration for SystemStream {} that is not true or false (was {})." +
        " Defaulting to false.", systemStream, resetOffset);

      resetOffset = "false";
    }
    return Boolean.valueOf(resetOffset);
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
  protected boolean containsSamzaProperty(SystemStream systemStream, String property) {
    if (!property.startsWith(SAMZA_PROPERTY)) {
      throw new IllegalArgumentException(
        String.format("Attempt to fetch a non samza property for SystemStream %s named %s", systemStream, property));
    }
    return getSamzaProperty(systemStream, property) != null;
  }

  public boolean isResetOffsetConfigured(SystemStream systemStream) {
    return containsSamzaProperty(systemStream, CONSUMER_RESET_OFFSET);
  }

  public Optional<String> getDefaultStreamOffset(SystemStream systemStream) {
    return Optional.ofNullable(getSamzaProperty(systemStream, CONSUMER_OFFSET_DEFAULT));
  }

  public boolean isDefaultStreamOffsetConfigured(SystemStream systemStream) {
    return containsSamzaProperty(systemStream, CONSUMER_OFFSET_DEFAULT);
  }

  public boolean getBootstrapEnabled(SystemStream systemStream) {
    return Boolean.parseBoolean(getSamzaProperty(systemStream, BOOTSTRAP));
  }

  public boolean getBroadcastEnabled(SystemStream systemStream) {
    return Boolean.parseBoolean(getSamzaProperty(systemStream, BROADCAST));
  }

  public int getPriority(SystemStream systemStream) {
    return Integer.parseInt(getSamzaProperty(systemStream, PRIORITY, "-1"));
  }

  /**
   * A streamId is translated to a SystemStream by looking up its System and physicalName. It
   * will use the streamId as the stream name if the physicalName doesn't exist.
   */
  public SystemStream streamIdToSystemStream(String streamId) {
    return new SystemStream(getSystem(streamId), getPhysicalName(streamId));
  }


  /**
   * Returns a list of all SystemStreams that have a serde defined from the config file.
   */
  public Set<SystemStream> getSerdeStreams(String systemName) {
    Config subConf = subset(String.format("systems.%s.streams.", systemName), true);
    Set<SystemStream> legacySystemStreams = subConf.keySet().stream()
      .filter(k -> k.endsWith(MSG_SERDE) || k.endsWith(KEY_SERDE))
      .map(k -> {
        String streamName = k.substring(0, k.length() - 16 /* .samza.XXX.serde length */);
        return new SystemStream(systemName, streamName);
      })
      .collect(Collectors.toSet());

    Set<SystemStream> systemStreams = subset(STREAMS_PREFIX).keySet().stream()
      .filter(k -> k.endsWith(MSG_SERDE) || k.endsWith(KEY_SERDE))
      .map(k -> k.substring(0, k.length() - 16 /* .samza.XXX.serde length */))
      .filter(streamId -> systemName.equals(getSystem(streamId)))
      .map(streamId -> streamIdToSystemStream(streamId)).collect(Collectors.toSet());

    return Sets.union(legacySystemStreams, systemStreams).immutableCopy();
  }

 /* Gets the properties for the streamId which are not Samza properties (i.e. do not have a "samza." prefix). This
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
  public Config getStreamProperties(String streamId) {
    MapConfig allProperties = getAllStreamProperties(streamId);
    Config samzaProperties = allProperties.subset(SAMZA_PROPERTY, false);
    Map<String, String> filteredStreamProperties =
      allProperties.entrySet().stream().filter(kv -> !samzaProperties.containsKey(kv.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return new MapConfig(filteredStreamProperties);
  }

  /**
   * Gets the boolean flag of whether the specified streamId is an intermediate stream
   *
   * @param streamId the identifier for the stream in the config.
   * @return true if the stream is intermediate
   */
  public boolean getIsIntermediateStream(String streamId) {
    return getBoolean(String.format(IS_INTERMEDIATE_FOR_STREAM_ID, streamId), false);
  }

  /**
   * Gets the boolean flag of whether the committed messages specified streamId can be deleted
   *
   * @param streamId the identifier for the stream in the config.
   * @return true if the committed messages of the stream can be deleted
   */
  public boolean getDeleteCommittedMessages(String streamId) {
    return getBoolean(String.format(DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID, streamId), false);
  }

  public boolean getIsBounded(String streamId) {
    return getBoolean(String.format(IS_BOUNDED_FOR_STREAM_ID, streamId), false);
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
   * 3) or null
   *
   * @param systemStream the SystemStream for which the property value will be retrieved.
   * @param property the samza property key (including the "samza." prefix); for example, for both
   *                 "streams.streamId.samza.prop.key" and "systems.system.streams.streamName.samza.prop.key", this
   *                 argument should have the value "samza.prop.key"
   * @return the property value
   */
  private String getSamzaProperty(SystemStream systemStream, String property) {
    if (!property.startsWith(SAMZA_PROPERTY)) {
      throw new IllegalArgumentException(
        String.format("Attempt to fetch a non samza property for SystemStream %s named %s", systemStream, property));
    }

    String streamVal = getAllStreamProperties(systemStreamToStreamId(systemStream)).get(property);
    return (streamVal != null) ? streamVal : getSystemStreamProperties(systemStream.getSystem(), systemStream.getStream()).get(property);
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
  private String getSamzaProperty(SystemStream systemStream, String property, String defaultValue) {
    String streamVal = getSamzaProperty(systemStream, property);

    return streamVal != null ? streamVal : defaultValue;
  }
}
