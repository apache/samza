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
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;


/**
 * A descriptor for the EventHubs output stream
 *<p>
 *   An instance of this descriptor may be obtained from an {@link EventHubSystemDescriptor}
 *</p>
 * Stream properties configured using a descriptor overrides corresponding properties and property defaults provided
 * in configuration.
 *
 * @param <StreamMessageType> type of messages in this stream
 */
public class EventHubInputDescriptor<StreamMessageType>
    extends InputDescriptor<StreamMessageType, EventHubInputDescriptor<StreamMessageType>> {
  private Optional<String> namespace = Optional.empty();
  private Optional<String> entityPath = Optional.empty();
  private Optional<String> sasKeyName = Optional.empty();
  private Optional<String> sasToken = Optional.empty();
  private Optional<String> consumerGroup = Optional.empty();

  /**
   * Constructs an {@link InputDescriptor} instance.
   *
   * @param streamId id of the stream
   * @param serde serde for messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   * @param transformer stream level input stream transform function if available, else null
   */
  public EventHubInputDescriptor(String streamId, Serde serde, SystemDescriptor systemDescriptor,
      InputTransformer transformer) {
    super(streamId, serde, systemDescriptor, transformer);
  }

  /**
   * Namespace associated with the input stream. Required to access the input Eventhubs entity per stream.
   *
   * @param namespace namespace of the Eventhub entity to consume from
   * @return this input descriptor
   */
  public EventHubInputDescriptor<StreamMessageType> withNamespace(String namespace) {
    this.namespace = Optional.of(namespace);
    return this;
  }

  /**
   * Entity path associated with the input stream. Required to access the input Eventhub entity per stream.
   *
   * @param path Entity path of the Eventhub entity to consume from
   * @return this input descriptor
   */
  public EventHubInputDescriptor<StreamMessageType> withEntityPath(String path) {
    this.entityPath = Optional.of(path);
    return this;
  }

  /**
   * SAS Key name of the associated input stream. Required to access the input Eventhub entity per stream.
   *
   * @param sasKeyName the name of the SAS key required to access the Eventhub entity
   * @return this input descriptor
   */
  public EventHubInputDescriptor<StreamMessageType> withSasKeyName(String sasKeyName) {
    this.sasKeyName = Optional.of(sasKeyName);
    return this;
  }

  /**
   * SAS Token of the associated input stream. Required to access the input Eventhub per stream.
   *
   * @param sasToken the SAS token required to access the Eventhub entity
   * @return this input descriptor
   */
  public EventHubInputDescriptor<StreamMessageType> withSasKey(String sasToken) {
    this.sasToken = Optional.of(sasToken);
    return this;
  }

  /**
   * Set the consumer group from the upstream Eventhub that the consumer is part of. Defaults to the
   * <code>$Default</code> group that is initially present in all Eventhub entities (unless removed)
   *
   * @param consumerGroup the name of the consumer group upstream
   * @return this input descriptor
   */
  public EventHubInputDescriptor<StreamMessageType> withConsumerGroup(String consumerGroup) {
    this.consumerGroup = Optional.of(StringUtils.stripToNull(consumerGroup));
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    HashMap<String, String> ehConfigs = new HashMap<>(super.toConfig());

    String streamId = getStreamId();
    String systemName = getSystemName();

    namespace.ifPresent(namespace ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, streamId), namespace));
    entityPath.ifPresent(path ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, streamId), path));
    sasKeyName.ifPresent(keyName ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, streamId), keyName));
    sasToken.ifPresent(key ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, streamId), key));
    this.consumerGroup.ifPresent(consumerGroupName ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_CONSUMER_GROUP, streamId), consumerGroupName));
    return ehConfigs;
  }

}
