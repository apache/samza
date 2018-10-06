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

import com.microsoft.azure.eventhubs.EventHubClient;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.operators.descriptors.base.system.OutputDescriptorProvider;
import org.apache.samza.operators.descriptors.base.system.SimpleInputDescriptorProvider;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer.PartitioningMethod;


/**
 * A descriptor for a EventHubs system.
 * <p>
 * System properties configured using a descriptor override corresponding properties provided in configuration.
 */
public class EventHubSystemDescriptor extends SystemDescriptor<EventHubSystemDescriptor>
    implements SimpleInputDescriptorProvider, OutputDescriptorProvider {
  private static final String FACTORY_CLASS_NAME = EventHubSystemFactory.class.getName();

  private List<String> streamIdList = Collections.emptyList();
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
  public EventHubSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, null, null);
  }

  /**
   *
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> EventHubOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId,
      Serde<StreamMessageType> serde) {
    return new EventHubOutputDescriptor<>(streamId, serde, this);
  }

  /**
   *
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> EventHubInputDescriptor<StreamMessageType> getInputDescriptor(String streamId,
      Serde<StreamMessageType> serde) {
    return new EventHubInputDescriptor<>(streamId, serde, this, null);
  }

  /**
   * Set the list of samza stream-ids that will be used with the Eventhub system
   *
   * @param streamList list of streamids of the Eventhub system
   * @return this system descriptor
   */
  public EventHubSystemDescriptor withStreamIds(List<String> streamList) {
    this.streamIdList = streamList;
    return this;
  }

  /**
   * Timeout for fetching the runtime metadata from an Eventhub entity on startup in millis.
   *
   * @param timeout the value in milliseconds for the timeout for the completion of
   *                {@link EventHubClient#getRuntimeInformation()}
   * @return this system descriptor
   */
  public EventHubSystemDescriptor withRuntimeInfoTimeout(int timeout) {
    this.fetchRuntimeInfoTimeout = Optional.of(timeout);
    return this;
  }

  /**
   * Number of threads in thread pool that will be used by the EventHubClient.
   *
   * @param numClientThreads the number of threads
   * @return this system descriptor
   */
  public EventHubSystemDescriptor withNumClientThreads(int numClientThreads) {
    this.numClientThreads = Optional.of(numClientThreads);
    return this;
  }

  /**
   *  Per partition capacity of the eventhubs consumer buffer - the blocking queue used for storing messages.
   *  Larger buffer capacity typically leads to better throughput but consumes more memory.
   *
   * @param receiveQueueSize the number of messages from Eventhubs that should be buffered in the
   *                      {@link org.apache.samza.util.BlockingEnvelopeMap}
   * @return this system descriptor
   */
  public EventHubSystemDescriptor withReceiveQueueSize(int receiveQueueSize) {
    this.consumerReceiveQueueSize = Optional.of(receiveQueueSize);
    return this;
  }

  /**
   * Maximum number of events that EventHub client can return in a receive call.
   *
   * @param count the number of max events per poll
   * @return this system descriptor
   */
  public EventHubSystemDescriptor withMaxEventCountPerPoll(int count) {
    this.consumerMaxEventCountPerPoll = Optional.of(count);
    return this;
  }

  /**
   * Number of events that EventHub client should prefetch from the server.
   *
   * @param count the number of events that should be prefetched.
   * @return this system descriptor
   */
  public EventHubSystemDescriptor withPrefetchCount(int count) {
    this.consumerPrefetchCount = Optional.of(count);
    return this;
  }


  /**
   * Configure the method that the message is partitioned for the downstream Eventhub in one of the following ways:
   * <ul>
   *   <li>ROUND_ROBIN:
   *   The message key and partition key are ignored and the message
   *   will be distributed in a round-robin fashion amongst all the partitions in the downstream EventHub.</li>
   *   <li>EVENT_HUB_HASHING:
   *   Employs the hashing mechanism in EventHubs to determine, based on the key of the message,
   *   which partition the message should go. Using this method still ensures that all the events with
   *   the same key are sent to the same partition in the event hub. If this option is chosen, the partition
   *   key used for the hash should be a string. If the partition key is not set, the message key is
   *   used instead.</li>
   *   <li>PARTITION_KEY_AS_PARTITION:
   *   Use the integer key specified by the partition key or key of the message to a specific partition
   *   on Eventhub. If the integer key is greater than the number of partitions in the destination Eventhub,
   *   a modulo operation will be performed to determine the resulting paritition.
   *   ie. if there are 6 partitions and the key is 9, the message will end up in partition 3.
   *   Similarly to EVENT_HUB_HASHING, if the partition key is not set the message key is used instead.</li>
   * </ul>
   * @param partitioningMethod
   * @return this system descriptor
   */
  public EventHubSystemDescriptor withPartitioningMethod(PartitioningMethod partitioningMethod) {
    this.producerEventhubsPartitioningMethod = Optional.of(partitioningMethod);
    return this;
  }

  /**
   *  Sending each message key to the eventhub in the properties of the AMQP message.
   *  If the Samza Eventhub consumer is used, this field is used as the message key if the partition key is not present.
   *
   * @param sendKeys Set to true if the message key should be send in the EventData properties, false otherwise
   * @return this system descriptor
   */
  public EventHubSystemDescriptor withSendKeys(Boolean sendKeys){
    this.producerEventhubsSendKey = Optional.of(sendKeys);
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    Map<String, String> ehConfigs = new HashMap<>(super.toConfig());
    String systemName = getSystemName();

    if(!this.streamIdList.isEmpty()) {
      ehConfigs.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), String.join(",", this.streamIdList));
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
