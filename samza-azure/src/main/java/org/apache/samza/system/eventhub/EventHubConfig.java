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
import com.microsoft.azure.eventhubs.PartitionReceiver;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;
import scala.collection.JavaConversions;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventHubConfig extends MapConfig {

  public static final String CONFIG_STREAM_LIST = "systems.%s.stream.list";

  public static final String CONFIG_STREAM_NAMESPACE = "streams.%s.eventhubs.namespace";

  public static final String CONFIG_STREAM_ENTITYPATH = "streams.%s.eventhubs.entitypath";

  public static final String CONFIG_STREAM_SAS_KEY_NAME = Config.SENSITIVE_PREFIX + "streams.%s.eventhubs.sas.keyname";

  public static final String CONFIG_STREAM_SAS_TOKEN = Config.SENSITIVE_PREFIX + "streams.%s.eventhubs.sas.token";

  public static final String CONFIG_SKIP_MESSAGES_LARGER_THAN = "systems.%s.eventhubs.skipMessagesLargerThanBytes";

  public static final String CONFIG_STREAM_CONSUMER_GROUP = "streams.%s.eventhubs.consumer.group";
  public static final String DEFAULT_CONFIG_STREAM_CONSUMER_GROUP = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME;

  public static final String CONFIG_SYSTEM_NUM_CLIENT_THREADS = "streams.%s.eventhubs.numClientThreads";
  public static final int DEFAULT_CONFIG_SYSTEM_NUM_CLIENT_THREADS = 10;

  public static final String CONFIG_PREFETCH_COUNT = "systems.%s.eventhubs.prefetchCount";
  public static final int DEFAULT_CONFIG_PREFETCH_COUNT = PartitionReceiver.DEFAULT_PREFETCH_COUNT;

  public static final String CONFIG_MAX_EVENT_COUNT_PER_POLL = "systems.%s.eventhubs.maxEventCountPerPoll";
  public static final int DEFAULT_CONFIG_MAX_EVENT_COUNT_PER_POLL = 50;

  public static final String CONFIG_PRODUCER_PARTITION_METHOD = "systems.%s.eventhubs.partition.method";
  public static final String DEFAULT_CONFIG_PRODUCER_PARTITION_METHOD = EventHubSystemProducer
          .PartitioningMethod.EVENT_HUB_HASHING.name();

  public static final String CONFIG_SEND_KEY_IN_EVENT_PROPERTIES = "systems.%s.eventhubs.send.key";
  public static final Boolean DEFAULT_CONFIG_SEND_KEY_IN_EVENT_PROPERTIES = true;

  public static final String CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS = "systems.%s.eventhubs.runtime.info.timeout";
  public static final long DEFAULT_CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  public static final String CONFIG_CONSUMER_BUFFER_CAPACITY = "systems.%s.eventhubs.receive.queue.size";
  public static final int DEFAULT_CONFIG_CONSUMER_BUFFER_CAPACITY = 100;

  // By default we will skip messages larger than 1MB.
  private static final int DEFAULT_MAX_MESSAGE_SIZE = 1024 * 1024;

  private final Map<String, String> physcialToId = new HashMap<>();

  public EventHubConfig(Config config) {
    super(config);

    // Build reverse index for streamName -> streamId
    StreamConfig streamConfig = new StreamConfig(config);
    JavaConversions.asJavaCollection(streamConfig.getStreamIds())
            .forEach((streamId) -> physcialToId.put(streamConfig.getPhysicalName(streamId), streamId));
  }

  private String getFromStreamIdOrName(String configName, String streamName, String defaultString) {
    String result = getFromStreamIdOrName(configName, streamName);
    if (result == null) {
      return defaultString;
    }
    return result;
  }

  private String getFromStreamIdOrName(String configName, String streamName) {
    String streamId = getStreamId(streamName);
    return get(String.format(configName, streamId),
            streamId.equals(streamName) ? null : get(String.format(configName, streamName)));
  }

  private String validateRequiredConfig(String value, String fieldName, String systemName, String streamName) {
    if (value == null) {
      throw new SamzaException(String.format("Missing %s configuration for system: %s, stream: %s",
              fieldName, systemName, streamName));
    }
    return value;
  }

  /**
   * Get the streamId for the specified streamName
   *
   * @param streamName the physical identifier of a stream
   * @return the streamId identifier for the stream or the queried streamName if it is not found.
   */
  public String getStreamId(String streamName) {
    return physcialToId.getOrDefault(streamName, streamName);
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
   * @param streamName name of stream (physical or streamId)
   * @return EventHubs namespace
   */
  public String getStreamNamespace(String systemName, String streamName) {
    return validateRequiredConfig(getFromStreamIdOrName(CONFIG_STREAM_NAMESPACE, streamName),
            "Namespace", systemName, streamName);
  }

  /**
   * Get the EventHubs entity path (topic name) for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream (physical or streamId)
   * @return EventHubs entity path
   */
  public String getStreamEntityPath(String systemName, String streamName) {
    return validateRequiredConfig(getFromStreamIdOrName(CONFIG_STREAM_ENTITYPATH, streamName),
            "EntityPath", systemName, streamName);
  }

  /**
   * Get the number of client threads, This is used to create the ThreadPool executor that is passed to the
   * {@link EventHubClient#create}
   * @param systemName Name of the system.
   * @return Num of client threads to use.
   */
  public Integer getNumClientThreads(String systemName) {
    return getInt(String.format(CONFIG_SYSTEM_NUM_CLIENT_THREADS, systemName), DEFAULT_CONFIG_SYSTEM_NUM_CLIENT_THREADS);
  }

  /**
   * Get the max event count returned per poll
   * @param systemName Name of the system
   * @return Max number of events returned per poll
   */
  public Integer getMaxEventCountPerPoll(String systemName) {
    return getInt(String.format(CONFIG_MAX_EVENT_COUNT_PER_POLL, systemName), DEFAULT_CONFIG_MAX_EVENT_COUNT_PER_POLL);
  }

  /**
   * Get the per partition prefetch count for the event hub client
   * @param systemName Name of the system.
   * @return Per partition Prefetch count for the event hub client.
   */
  public Integer getPrefetchCount(String systemName) {
    return getInt(String.format(CONFIG_PREFETCH_COUNT, systemName), DEFAULT_CONFIG_PREFETCH_COUNT);
  }

  /**
   * Get the EventHubs max Message size
   *
   * @param systemName name of the system
   * @return the max message size supported in event hubs.
   */
  public Integer getSkipMessagesLargerThan(String systemName) {
    return getInt(String.format(CONFIG_SKIP_MESSAGES_LARGER_THAN, systemName), DEFAULT_MAX_MESSAGE_SIZE);
  }

  /**
   * Get the EventHubs SAS (Shared Access Signature) key name for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream (physical or streamId)
   * @return EventHubs SAS key name
   */
  public String getStreamSasKeyName(String systemName, String streamName) {
    return validateRequiredConfig(getFromStreamIdOrName(CONFIG_STREAM_SAS_KEY_NAME, streamName),
            "SASKeyName", systemName, streamName);
  }

  /**
   * Get the EventHubs SAS (Shared Access Signature) token for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream (physical or streamId)
   * @return EventHubs SAS token
   */
  public String getStreamSasToken(String systemName, String streamName) {
    return validateRequiredConfig(getFromStreamIdOrName(CONFIG_STREAM_SAS_TOKEN, streamName),
            "SASToken", systemName, streamName);
  }

  /**
   * Get the EventHubs consumer group used for consumption for the stream
   *
   * @param systemName name of the system
   * @param streamName name of stream (physical or streamId)
   * @return EventHubs consumer group
   */
  public String getStreamConsumerGroup(String systemName, String streamName) {
    return getFromStreamIdOrName(CONFIG_STREAM_CONSUMER_GROUP, streamName, DEFAULT_CONFIG_STREAM_CONSUMER_GROUP);
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
