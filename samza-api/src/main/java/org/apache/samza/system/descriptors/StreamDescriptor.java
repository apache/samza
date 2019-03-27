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
package org.apache.samza.system.descriptors;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.serializers.Serde;

/**
 * A {@link StreamDescriptor} can be used for specifying Samza and system-specific properties of input/output streams.
 * <p>
 * Stream properties provided in configuration override corresponding properties specified using a descriptors.
 * <p>
 * This is the base descriptor for an input/output stream. Use a system-specific input/output descriptor
 * (e.g. KafkaInputDescriptor) obtained from its system descriptor (e.g. KafkaSystemDescriptor) if one is available.
 * Otherwise use the {@link GenericInputDescriptor} and {@link GenericOutputDescriptor} obtained from a
 * {@link GenericSystemDescriptor}.
 *
 * @param <StreamMessageType> type of messages in this stream.
 * @param <SubClass> type of the concrete sub-class
 */
public abstract class StreamDescriptor<StreamMessageType, SubClass extends StreamDescriptor<StreamMessageType, SubClass>> {
  private static final String SYSTEM_CONFIG_KEY = "streams.%s.samza.system";
  private static final String PHYSICAL_NAME_CONFIG_KEY = "streams.%s.samza.physical.name";
  private static final String STREAM_CONFIGS_CONFIG_KEY = "streams.%s.%s";
  private static final Pattern STREAM_ID_PATTERN = Pattern.compile("[\\d\\w-_]+");

  private final String streamId;
  private final Serde serde;
  private final SystemDescriptor systemDescriptor;

  private final Map<String, String> streamConfigs = new HashMap<>();
  private Optional<String> physicalNameOptional = Optional.empty();

  /**
   * Constructs a {@link StreamDescriptor} instance.
   *
   * @param streamId id of the stream
   * @param serde serde for messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  StreamDescriptor(String streamId, Serde serde, SystemDescriptor systemDescriptor) {
    Preconditions.checkArgument(systemDescriptor != null,
        String.format("SystemDescriptor must not be null. streamId: %s", streamId));
    String systemName = systemDescriptor.getSystemName();
    Preconditions.checkState(isValidStreamId(streamId),
        String.format("streamId must be non-empty and must not contain spaces or special characters. " +
            "streamId: %s, systemName: %s", streamId, systemName));
    Preconditions.checkArgument(serde != null,
        String.format("Serde must not be null. streamId: %s systemName: %s", streamId, systemName));
    this.streamId = streamId;
    this.serde = serde;
    this.systemDescriptor = systemDescriptor;
  }

  /**
   * The physical name of the stream on the system on which this stream will be accessed.
   * This is opposed to the {@code streamId} which is the logical name that Samza uses to identify the stream.
   * <p>
   * A physical name could be a Kafka topic name, an HDFS file URN, or any other system-specific identifier.
   * <p>
   * If not provided, the logical {@code streamId} is used as the physical name.
   *
   * @param physicalName physical name for this stream.
   * @return this stream descriptor.
   */
  public SubClass withPhysicalName(String physicalName) {
    this.physicalNameOptional = Optional.ofNullable(physicalName);
    return (SubClass) this;
  }

  /**
   * Additional system-specific properties for this stream.
   * <p>
   * These properties are added under the {@code streams.stream-id.*} scope.
   *
   * @param streamConfigs system-specific properties for this stream
   * @return this stream descriptor
   */
  public SubClass withStreamConfigs(Map<String, String> streamConfigs) {
    this.streamConfigs.putAll(streamConfigs);
    return (SubClass) this;
  }

  public String getStreamId() {
    return this.streamId;
  }

  public String getSystemName() {
    return this.systemDescriptor.getSystemName();
  }

  public Serde getSerde() {
    return this.serde;
  }

  public SystemDescriptor getSystemDescriptor() {
    return this.systemDescriptor;
  }

  public Optional<String> getPhysicalName() {
    return physicalNameOptional;
  }

  private boolean isValidStreamId(String id) {
    return StringUtils.isNotBlank(id) && STREAM_ID_PATTERN.matcher(id).matches();
  }

  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>();
    configs.put(String.format(SYSTEM_CONFIG_KEY, streamId), getSystemName());
    this.physicalNameOptional.ifPresent(physicalName ->
        configs.put(String.format(PHYSICAL_NAME_CONFIG_KEY, streamId), physicalName));
    this.streamConfigs.forEach((key, value) ->
        configs.put(String.format(STREAM_CONFIGS_CONFIG_KEY, streamId, key), value));
    return Collections.unmodifiableMap(configs);
  }
}
