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
package org.apache.samza.system.eventhub;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.serializers.Serde;

/**
 * A descriptor for an EventHubs input stream
 * <p>
 *   An instance of this descriptor may be obtained from and {@link EventHubSystemDescriptor}
 * </p>
 * Stream properties configured using a descriptor overrides corresponding properties and property defaults provided
 * in configuration.
 *
 * @param <StreamMessageType> type of messages in this stream
 */
public class EventHubOutputDescriptor<StreamMessageType>
    extends OutputDescriptor<StreamMessageType, EventHubOutputDescriptor<StreamMessageType>> {
  private Optional<String> namespace = Optional.empty();
  private Optional<String> entityPath = Optional.empty();
  private Optional<String> sasKeyName = Optional.empty();
  private Optional<String> sasToken = Optional.empty();

  /**
   * Constructs an {@link OutputDescriptor} instance.
   *
   * @param streamId id of the stream
   * @param serde serde for messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  public EventHubOutputDescriptor(String streamId, Serde<StreamMessageType> serde, SystemDescriptor systemDescriptor) {
    super(streamId, serde, systemDescriptor);
  }

  /**
   * Namespace associated with the output stream. Required to access the output Eventhubs entity per stream.
   *
   * @param namespace namespace of the Eventhub entity to produce to
   * @returne this output descriptor
   */
  public EventHubOutputDescriptor<StreamMessageType> withNamespace(String namespace) {
    this.namespace = Optional.of(namespace);
    return this;
  }

  /**
   * Entity path associated with the output stream. Required to access the output Eventhub entity per stream.
   *
   * @param path path name of the Eventhub entity to produce to
   * @return this output descriptor
   */
  public EventHubOutputDescriptor<StreamMessageType> withEntityPath(String path) {
    this.entityPath = Optional.of(path);
    return this;
  }

  /**
   * SAS Key name of the associated output stream. Required to access the output Eventhub entity per stream.
   *
   * @param sasKeyName the name of the SAS key required to access the Eventhub entity
   * @return this output descriptor
   */
  public EventHubOutputDescriptor<StreamMessageType> withSasKeyName(String sasKeyName) {
    this.sasKeyName = Optional.of(sasKeyName);
    return this;
  }

  /**
   * SAS Token of the associated output stream. Required to access the output Eventhub per stream.
   *
   * @param sasToken the SAS token required to access to Eventhub entity
   * @return this output descriptor
   */
  public EventHubOutputDescriptor<StreamMessageType> withSasKey(String sasToken) {
    this.sasToken = Optional.of(sasToken);
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    HashMap<String, String> ehConfigs = new HashMap<>(super.toConfig());

    String streamId = getStreamId();

    namespace.ifPresent(namespace ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, streamId), namespace));
    entityPath.ifPresent(path ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, streamId), path));
    sasKeyName.ifPresent(keyName ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, streamId), keyName));
    sasToken.ifPresent(key ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, streamId), key));
    return ehConfigs;
  }

}
