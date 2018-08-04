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
package org.apache.samza.operators.descriptors.base.stream;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.serializers.Serde;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The base descriptor for an input or output stream. Allows setting properties that are common to all streams.
 *
 * @param <StreamMessageType> type of messages in this stream.
 * @param <SubClass> type of the concrete sub-class
 */
@SuppressWarnings("unchecked")
public abstract class StreamDescriptor<StreamMessageType, SubClass extends StreamDescriptor<StreamMessageType, SubClass>> {
  private static final String SYSTEM_CONFIG_KEY = "streams.%s.samza.system";
  private static final String PHYSICAL_NAME_CONFIG_KEY = "streams.%s.samza.physical.name";
  private static final String STREAM_CONFIGS_CONFIG_KEY = "streams.%s.%s";

  private final String streamId;
  private final String systemName;
  private final Serde serde;
  private final Optional<SystemDescriptor> systemDescriptorOptional;

  private Optional<String> physicalNameOptional = Optional.empty();
  private Map<String, String> streamConfigs = new HashMap<>();

  /**
   * Constructs a {@link StreamDescriptor} instance.
   *
   * @param streamId id of the stream
   * @param systemName system name for the stream
   * @param serde serde for messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from if available, else null
   */
  StreamDescriptor(String streamId, String systemName, Serde<StreamMessageType> serde,
      SystemDescriptor systemDescriptor) {
    this.streamId = streamId;
    this.systemName = systemName;
    if (serde == null) {
      throw new SamzaException(
          String.format("Serde must not be null for stream: %s on system: %s", streamId, systemName));
    }
    this.serde = serde;
    if (systemDescriptor != null) {
      if (!systemDescriptor.getSystemName().equals(systemName)) {
        throw new SamzaException(
            String.format("System name in constructor: %s does not match system name in SystemDescriptor: %s",
                systemName, systemDescriptor.getSystemName()));
      }
      this.systemDescriptorOptional = Optional.of(systemDescriptor);
    } else {
      this.systemDescriptorOptional = Optional.empty();
    }
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
    return this.systemName;
  }

  /**
   * Get the serde for this stream. This can be the stream level serde if one was provided,
   * or the default system level serde.
   *
   * @return the serde for this stream
   */
  public Serde getSerde() {
    return this.serde;
  }

  public Optional<SystemDescriptor> getSystemDescriptor() {
    return this.systemDescriptorOptional;
  }

  public Optional<String> getPhysicalName() {
    return physicalNameOptional;
  }

  private Map<String, String> getStreamConfigs() {
    return this.streamConfigs;
  }

  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>();
    configs.put(String.format(SYSTEM_CONFIG_KEY, streamId), getSystemName());
    getPhysicalName().ifPresent(physicalName ->
        configs.put(String.format(PHYSICAL_NAME_CONFIG_KEY, streamId), physicalName));
    getStreamConfigs().forEach((key, value) ->
        configs.put(String.format(STREAM_CONFIGS_CONFIG_KEY, streamId, key), value));
    return Collections.unmodifiableMap(configs);
  }
}
