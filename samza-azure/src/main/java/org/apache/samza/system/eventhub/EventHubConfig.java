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
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class EventHubConfig extends MapConfig {

  public static final String CONFIG_STREAM_LIST = "systems.%s.stream.list";

  public static final String CONFIG_STREAM_NAMESPACE = "systems.%s.streams.%s.eventhubs.namespace";

  public static final String CONFIG_STREAM_ENTITYPATH = "systems.%s.streams.%s.eventhubs.entitypath";

  public static final String CONFIG_STREAM_SAS_KEY_NAME = "systems.%s.streams.%s.eventhubs.sas.keyname";

  public static final String CONFIG_STREAM_SAS_TOKEN = "systems.%s.streams.%s.eventhubs.sas.token";

  public static final String CONFIG_STREAM_CONSUMER_GROUP = "systems.%s.streams.%s.eventhubs.consumer.group";
  public static final String DEFAULT_CONFIG_STREAM_CONSUMER_GROUP = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME;

  public static final String CONFIG_PRODUCER_PARTITION_METHOD = "systems.%s.eventhubs.partition.method";
  public static final String DEFAULT_CONFIG_PRODUCER_PARTITION_METHOD = EventHubSystemProducer
          .PartitioningMethod.EVENT_HUB_HASHING.name();

  public static final String CONFIG_SEND_KEY_IN_EVENT_PROPERTIES = "systems.%s.eventhubs.send.key";
  public static final Boolean DEFAULT_CONFIG_SEND_KEY_IN_EVENT_PROPERTIES = false;

  public static final String CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS = "systems.%s.eventhubs.runtime.info.timeout";
  public static final long DEFAULT_CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  public static final String CONFIG_CONSUMER_BUFFER_CAPACITY = "systems.%s.eventhubs.receive.queue.size";
  public static final int DEFAULT_CONFIG_CONSUMER_BUFFER_CAPACITY = 100;


  public EventHubConfig(Map<String, String> config) {
    super(config);
  }

  /**
   * Get the list of streams that are defined. Each stream has enough
   * information for connecting to a certain EventHub entity.
   *
   * @param systemName name of the system
   * @return list of stream names
   */
  public List<String> getStreams(String systemName) {
    return getList(String.format(CONFIG_STREAM_LIST, systemName));
  }

  /**
   * Get the EventHubs namespace for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream
   * @return EventHubs namespace
   */
  public String getStreamNamespace(String systemName, String streamName) {
    return get(String.format(CONFIG_STREAM_NAMESPACE, systemName, streamName));
  }

  /**
   * Get the EventHubs entity path (topic name) for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream
   * @return EventHubs entity path
   */
  public String getStreamEntityPath(String systemName, String streamName) {
    return get(String.format(CONFIG_STREAM_ENTITYPATH, systemName, streamName));
  }

  /**
   * Get the EventHubs SAS (Shared Access Signature) key name for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream
   * @return EventHubs SAS key name
   */
  public String getStreamSasKeyName(String systemName, String streamName) {
    return get(String.format(CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName));
  }

  /**
   * Get the EventHubs SAS (Shared Access Signature) token for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream
   * @return EventHubs SAS token
   */
  public String getStreamSasToken(String systemName, String streamName) {
    return get(String.format(CONFIG_STREAM_SAS_TOKEN, systemName, streamName));
  }

  /**
   * Get the EventHubs consumer group used for consumption for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream
   * @return EventHubs consumer group
   */
  public String getStreamConsumerGroup(String systemName, String streamName) {
    return get(String.format(CONFIG_STREAM_CONSUMER_GROUP, systemName, streamName), DEFAULT_CONFIG_STREAM_CONSUMER_GROUP);
  }

  /**
   * Get the partition method of the systemName. By default partitioning is handed by EventHub.
   *
   * @param systemName name of the system
   * @return The method the producer should use to partition the outgoing data
   */
  public EventHubSystemProducer.PartitioningMethod getPartitioningMethod(String systemName) {
    String partitioningMethod = get(String.format(CONFIG_PRODUCER_PARTITION_METHOD, systemName),
            DEFAULT_CONFIG_PRODUCER_PARTITION_METHOD);
    return EventHubSystemProducer.PartitioningMethod.valueOf(partitioningMethod);

  }

  /**
   * Returns true if the OutgoingMessageEnvelope key should be sent in the outgoing envelope, false otherwise
   *
   * @param systemName name of the system
   * @return Boolean, is send key included
   */
  public Boolean getSendKeyInEventProperties(String systemName) {
    String isSendKeyIncluded = get(String.format(CONFIG_SEND_KEY_IN_EVENT_PROPERTIES, systemName));
    if (isSendKeyIncluded == null) {
      return DEFAULT_CONFIG_SEND_KEY_IN_EVENT_PROPERTIES;
    }
    return Boolean.valueOf(isSendKeyIncluded);
  }

  /**
   * Get the timeout for the getRuntimeInfo request to EventHub client
   *
   * @param systemName name of the systems
   * @return long, timeout in millis for fetching RuntimeInfo
   */
  public long getRuntimeInfoWaitTimeMS(String systemName) {
    return getLong(String.format(CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS, systemName),
            DEFAULT_CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS);
  }

  /**
   * Get the capacity of the Event Hub consumer buffer - the blocking queue used for storing messages
   *
   * @param systemName name of the system
   * @return int, number of buffered messages per SystemStreamPartition
   */
  public int getConsumerBufferCapacity(String systemName) {
    String bufferCapacity = get(String.format(CONFIG_CONSUMER_BUFFER_CAPACITY, systemName));
    if (bufferCapacity == null) {
      return DEFAULT_CONFIG_CONSUMER_BUFFER_CAPACITY;
    }
    return Integer.parseInt(bufferCapacity);
  }

}
