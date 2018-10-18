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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.operators.KV;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.EventHubSystemFactory;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer.PartitioningMethod;


/**
 * A {@link EventHubsSystemDescriptor} can be used for specifying Samza and EventHubs-specific properties of a EventHubs
 * input/output system. It can also be used for obtaining {@link EventHubsInputDescriptor}s and
 * {@link EventHubsOutputDescriptor}s, which can be used for specifying Samza and system-specific properties of
 * EventHubs input/output streams.
 * <p>
 * System properties provided in configuration override corresponding properties specified using a descriptor.
 */
public class EventHubsSystemDescriptor extends SystemDescriptor<EventHubsSystemDescriptor> {
  private static final String FACTORY_CLASS_NAME = EventHubSystemFactory.class.getName();

  private List<String> streamIds = new ArrayList<>();
  private Optional<Integer> fetchRuntimeInfoTimeout = Optional.empty();
  private Optional<Integer> numClientThreads = Optional.empty();
  private Optional<Integer> consumerReceiveQueueSize = Optional.empty();
  private Optional<Integer> consumerMaxEventCountPerPoll = Optional.empty();
  private Optional<Integer> consumerPrefetchCount = Optional.empty();
  private Optional<Boolean> producerEventhubsSendKey = Optional.empty();
  private Optional<PartitioningMethod> producerEventhubsPartitioningMethod = Optional.empty();

  /**
   * Constructs a {@link SystemDescriptor} instance.
   *  @param systemName name of this system
   */
  public EventHubsSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, null, null);
  }

  /**
   * Gets an {@link EventHubsInputDescriptor} for the input stream of this system. The stream has the provided
   * namespace and entity name of the associated Event Hubs entity and the provided stream level value serde.
   * <p>
   * The message in the stream will have {@link String} keys and {@code ValueType} values.
   *
   * @param streamId id of the input stream
   * @param namespace namespace of the Event Hubs entity to consume from
   * @param entityPath entity path of the Event Hubs entity to consume from
   * @param valueSerde stream level serde for the values in the messages in the input stream
   * @param <ValueType> type of the value in the messages in this stream
   * @return an {@link EventHubsInputDescriptor} for the Event Hubs input stream
   */
  public <ValueType> EventHubsInputDescriptor<KV<String, ValueType>> getInputDescriptor(String streamId, String namespace,
      String entityPath, Serde<ValueType> valueSerde) {
    streamIds.add(streamId);
    return new EventHubsInputDescriptor<>(streamId, namespace, entityPath, valueSerde, this);
  }

  /**
   * Gets an {@link EventHubsOutputDescriptor} for the output stream of this system. The stream has the provided
   * namespace and entity name of the associated Event Hubs entity and the provided stream level value serde.
   * <p>
   * The message in the stream will have {@link String} keys and {@code ValueType} values.
   *
   * @param streamId id of the output stream
   * @param namespace namespace of the Event Hubs entity to produce to
   * @param entityPath entity path of the Event Hubs entity to produce to
   * @param valueSerde stream level serde for the values in the messages to the output stream
   * @param <ValueType> type of the value in the messages in this stream
   * @return an {@link EventHubsOutputDescriptor} for the Event Hubs output stream
   */
  public <ValueType> EventHubsOutputDescriptor<KV<String, ValueType>> getOutputDescriptor(String streamId, String namespace,
      String entityPath, Serde<ValueType> valueSerde) {
    streamIds.add(streamId);
    return new EventHubsOutputDescriptor<>(streamId, namespace, entityPath, valueSerde, this);
  }

  /**
   * Timeout for fetching the runtime metadata from an Event Hubs entity on startup in millis.
   *
   * @param timeoutMS the timeout in ms for getting runtime information from the Event Hubs system
   * @return this system descriptor
   */
  public EventHubsSystemDescriptor withRuntimeInfoTimeout(int timeoutMS) {
    this.fetchRuntimeInfoTimeout = Optional.of(timeoutMS);
    return this;
  }

  /**
   * Number of threads in thread pool that will be used by the EventHubClient.
   *
   * @param numClientThreads the number of threads
   * @return this system descriptor
   */
  public EventHubsSystemDescriptor withNumClientThreads(int numClientThreads) {
    this.numClientThreads = Optional.of(numClientThreads);
    return this;
  }

  /**
   *  Per partition capacity of the Event Hubs consumer buffer - the blocking queue used for storing messages.
   *  Larger buffer capacity typically leads to better throughput but consumes more memory.
   *
   * @param receiveQueueSize the number of messages from Event Hubs that should be buffered in the
   *                      {@link org.apache.samza.util.BlockingEnvelopeMap}
   * @return this system descriptor
   */
  public EventHubsSystemDescriptor withReceiveQueueSize(int receiveQueueSize) {
    this.consumerReceiveQueueSize = Optional.of(receiveQueueSize);
    return this;
  }

  /**
   * Maximum number of events that Event Hubs client can return in a receive call.
   *
   * @param count the number of max events per poll
   * @return this system descriptor
   */
  public EventHubsSystemDescriptor withMaxEventCountPerPoll(int count) {
    this.consumerMaxEventCountPerPoll = Optional.of(count);
    return this;
  }

  /**
   * Number of events that Event Hubs client should prefetch from the server.
   *
   * @param count the number of events that should be prefetched.
   * @return this system descriptor
   */
  public EventHubsSystemDescriptor withPrefetchCount(int count) {
    this.consumerPrefetchCount = Optional.of(count);
    return this;
  }


  /**
   * Configure the method that the message is partitioned for the downstream Event Hubs in one of the following ways:
   * <ul>
   *   <li>ROUND_ROBIN:
   *   The message key and partition key are ignored and the message
   *   will be distributed in a round-robin fashion amongst all the partitions in the downstream Event Hubs entity.</li>
   *   <li>EVENT_HUB_HASHING:
   *   Employs the hashing mechanism in Event Hubs to determine, based on the key of the message,
   *   which partition the message should go. Using this method still ensures that all the events with
   *   the same key are sent to the same partition in the event hub. If this option is chosen, the partition
   *   key used for the hash should be a string. If the partition key is not set, the message key is
   *   used instead.</li>
   *   <li>PARTITION_KEY_AS_PARTITION:
   *   Use the integer key specified by the partition key or key of the message to a specific partition
   *   on Event Hubs. If the integer key is greater than the number of partitions in the destination Event Hubs entity,
   *   a modulo operation will be performed to determine the resulting paritition.
   *   ie. if there are 6 partitions and the key is 9, the message will end up in partition 3.
   *   Similarly to EVENT_HUB_HASHING, if the partition key is not set the message key is used instead.</li>
   * </ul>
   * @param partitioningMethod the desired partitioning method for the message in the downstream Event Hubs entity
   * @return this system descriptor
   */
  public EventHubsSystemDescriptor withPartitioningMethod(PartitioningMethod partitioningMethod) {
    this.producerEventhubsPartitioningMethod = Optional.ofNullable(partitioningMethod);
    return this;
  }

  /**
   *  If set to true, the key of the Samza message will be included as the 'key' property in the outgoing EventData
   *  message for Event Hubs. The Samza message key will not be sent otherwise.
   *  Note: If the Samza Event Hubs consumer is used, this field is the partition key of the received EventData, or the
   *  message key if the partition key is not present.
   *
   * @param sendKeys set to true if the message key should be sent in the EventData properties, the key is not sent otherwise
   * @return this system descriptor
   */
  public EventHubsSystemDescriptor withSendKeys(boolean sendKeys) {
    this.producerEventhubsSendKey = Optional.of(sendKeys);
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    Map<String, String> ehConfigs = new HashMap<>(super.toConfig());
    String systemName = getSystemName();

    if (!this.streamIds.isEmpty()) {
      ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), String.join(",", this.streamIds));
    }
    this.fetchRuntimeInfoTimeout.ifPresent(timeout ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS, systemName), Integer.toString(timeout)));
    this.numClientThreads.ifPresent(numClientThreads ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_SYSTEM_NUM_CLIENT_THREADS, systemName), Integer.toString(numClientThreads)));
    this.consumerReceiveQueueSize.ifPresent(receiveQueueSize ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_CONSUMER_BUFFER_CAPACITY, systemName), Integer.toString(receiveQueueSize)));
    this.consumerMaxEventCountPerPoll.ifPresent(count ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_MAX_EVENT_COUNT_PER_POLL, systemName), Integer.toString(count)));
    this.consumerPrefetchCount.ifPresent(count ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_PREFETCH_COUNT, systemName), Integer.toString(count)));
    this.producerEventhubsSendKey.ifPresent(sendKeys ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_SEND_KEY_IN_EVENT_PROPERTIES, systemName), Boolean.toString(sendKeys)));
    this.producerEventhubsPartitioningMethod.ifPresent(partitioningMethod ->
        ehConfigs.put(String.format(EventHubConfig.CONFIG_PRODUCER_PARTITION_METHOD, systemName), partitioningMethod.toString()));
    return ehConfigs;
  }
}
