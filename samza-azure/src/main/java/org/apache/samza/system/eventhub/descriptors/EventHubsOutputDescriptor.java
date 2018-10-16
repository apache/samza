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
package org.apache.samza.system.eventhub.descriptors;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.ConfigException;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.eventhub.EventHubConfig;

/**
 * A descriptor for an Event Hubs output stream
 * <p>
 * An instance of this descriptor may be obtained from and {@link EventHubsSystemDescriptor}
 * <p>
 * Stream properties provided in configuration override corresponding properties configured using a descriptor.
 *
 * @param <StreamMessageType> type of messages in this stream
 */
public class EventHubsOutputDescriptor<StreamMessageType>
    extends OutputDescriptor<StreamMessageType, EventHubsOutputDescriptor<StreamMessageType>> {
  private String namespace;
  private String entityPath;
  private Optional<String> sasKeyName = Optional.empty();
  private Optional<String> sasToken = Optional.empty();

  /**
   * Constructs an {@link OutputDescriptor} instance.
   *
   * @param streamId id of the stream
   * @param namespace namespace for the Event Hubs entity to produce to, not null
   * @param entityPath entity path for the Event Hubs entity to produce to, not null
   * @param valueSerde serde the values of the messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  EventHubsOutputDescriptor(String streamId, String namespace, String entityPath, Serde valueSerde,
      SystemDescriptor systemDescriptor) {
    super(streamId, KVSerde.of(new NoOpSerde<>(), valueSerde), systemDescriptor);
    this.namespace = StringUtils.stripToNull(namespace);
    this.entityPath = StringUtils.stripToNull(entityPath);
    if (this.namespace == null || this.entityPath == null) {
      throw new ConfigException(String.format("Missing namespace and entity path Event Hubs output descriptor in " //
          + "system: {%s}, stream: {%s}", getSystemName(), streamId));
    }
  }

  /**
   * SAS Key name of the associated output stream. Required to access the output Event Hubs entity per stream.
   *
   * @param sasKeyName the name of the SAS key required to access the Event Hubs entity
   * @return this output descriptor
   */
  public EventHubsOutputDescriptor<StreamMessageType> withSasKeyName(String sasKeyName) {
    this.sasKeyName = Optional.of(StringUtils.stripToNull(sasKeyName));
    return this;
  }

  /**
   * SAS Token of the associated output stream. Required to access the output Event Hubs per stream.
   *
   * @param sasToken the SAS token required to access to Event Hubs entity
   * @return this output descriptor
   */
  public EventHubsOutputDescriptor<StreamMessageType> withSasKey(String sasToken) {
    this.sasToken = Optional.of(StringUtils.stripToNull(sasToken));
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    HashMap<String, String> ehConfigs = new HashMap<>(super.toConfig());

    String streamId = getStreamId();

    ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, streamId), namespace);
    ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, streamId), entityPath);
    sasKeyName.ifPresent(keyName ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, streamId), keyName));
    sasToken.ifPresent(key ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, streamId), key));
    return ehConfigs;
  }

}
