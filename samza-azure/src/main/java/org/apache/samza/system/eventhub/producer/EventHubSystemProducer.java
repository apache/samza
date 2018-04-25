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

package org.apache.samza.system.eventhub.producer;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.eventhub.EventHubClientManager;
import org.apache.samza.system.eventhub.EventHubClientManagerFactory;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.eventhubs.impl.ClientConstants;
import com.microsoft.azure.eventhubs.impl.EventDataImpl;


/**
 * EventHub system producer that can be used in Samza jobs to send events to Azure EventHubs
 */
public class EventHubSystemProducer extends AsyncSystemProducer {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemProducer.class.getName());
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  public static final String PRODUCE_TIMESTAMP = "produce-timestamp";
  public static final String KEY = "key";

  // Metrics recording
  private static final String EVENT_SKIP_RATE = "eventSkipRate";
  private static final String EVENT_WRITE_RATE = "eventWriteRate";
  private static final String EVENT_BYTE_WRITE_RATE = "eventByteWriteRate";

  private static final Object AGGREGATE_METRICS_LOCK = new Object();

  public enum PartitioningMethod {
    ROUND_ROBIN, EVENT_HUB_HASHING, PARTITION_KEY_AS_PARTITION
  }

  /**
   * Per stream metrics. Key is the stream Name.
   */
  private final HashMap<String, Counter> eventSkipRate = new HashMap<>();
  private final HashMap<String, Counter> eventWriteRate = new HashMap<>();
  private final HashMap<String, Counter> eventByteWriteRate = new HashMap<>();

  /**
   * Aggregated metrics.
   */
  private static Counter aggEventSkipRate = null;
  private static Counter aggEventWriteRate = null;
  private static Counter aggEventByteWriteRate = null;

  private final EventHubConfig config;
  private final PartitioningMethod partitioningMethod;
  private final String systemName;
  private final int maxMessageSize;

  private volatile boolean isStarted = false;

  /**
   * Per partition event hub client. Partitions from the same stream may share the same client,
   * depends on config PerPartitionConnection. See {@link EventHubConfig}
   */
  @VisibleForTesting
  final Map<String, Map<Integer, EventHubClientManager>> perPartitionEventHubClients = new HashMap<>();

  private final Map<String, EventHubClientManager> perStreamEventHubClientManagers = new HashMap<>();

  /**
   * PartitionSender for each partition in the stream.
   */
  private final Map<String, Map<Integer, PartitionSender>> streamPartitionSenders = new HashMap<>();

  /**
   * Per stream message interceptors
   */
  private final Map<String, Interceptor> interceptors;

  private final EventHubClientManagerFactory eventHubClientManagerFactory;

  public EventHubSystemProducer(EventHubConfig config, String systemName,
      EventHubClientManagerFactory eventHubClientManagerFactory, Map<String, Interceptor> interceptors,
      MetricsRegistry registry) {
    super(systemName, config, registry);
    LOG.info("Creating EventHub Producer for system {}", systemName);
    this.config = config;
    this.systemName = systemName;
    this.partitioningMethod = config.getPartitioningMethod(systemName);
    this.interceptors = interceptors;
    this.maxMessageSize = config.getSkipMessagesLargerThan(systemName);
    this.eventHubClientManagerFactory = eventHubClientManagerFactory;
    // Fetches the stream ids
    List<String> streamIds = config.getStreams(systemName);

    // Create and initiate connections to Event Hubs
    // even if PerPartitionConnection == true, we still need a stream level event hub for initial metadata (fetching
    // partition count)
    for (String streamId : streamIds) {
      EventHubClientManager ehClient =
          eventHubClientManagerFactory.getEventHubClientManager(systemName, streamId, config);
      perStreamEventHubClientManagers.put(streamId, ehClient);
      ehClient.init();
    }
  }

  @Override
  public synchronized void register(String source) {
    LOG.info("Registering source {}", source);
    if (isStarted) {
      String msg = "Cannot register once the producer is started.";
      throw new SamzaException(msg);
    }
  }

  private EventHubClientManager createOrGetEventHubClientManagerForPartition(String streamId, int partitionId) {
    Map<Integer, EventHubClientManager> perStreamMap =
        perPartitionEventHubClients.computeIfAbsent(streamId, key -> new HashMap<>());
    EventHubClientManager eventHubClientManager;
    if (config.getPerPartitionConnection(systemName)) {
      // will create one EventHub client per partition
      if (perStreamMap.containsKey(partitionId)) {
        LOG.warn(String.format("Trying to create new EventHubClientManager for partition=%d. But one already exists",
            partitionId));
        eventHubClientManager = perStreamMap.get(partitionId);
      } else {
        LOG.info(
            String.format("Creating EventHub client manager for streamId=%s, partitionId=%d: ", streamId, partitionId));
        eventHubClientManager = eventHubClientManagerFactory.getEventHubClientManager(systemName, streamId, config);
        eventHubClientManager.init();
        perStreamMap.put(partitionId, eventHubClientManager);
      }
    } else {
      // will share one EventHub client per stream
      eventHubClientManager = perStreamEventHubClientManagers.get(streamId);
      perStreamMap.put(partitionId, eventHubClientManager);
    }
    Validate.notNull(eventHubClientManager,
        String.format("Fail to create or get EventHubClientManager for streamId=%s, partitionId=%d", streamId,
            partitionId));
    return eventHubClientManager;
  }

  @Override
  public synchronized void start() {
    super.start();
    LOG.info("Starting system producer.");

    // Create partition senders if required
    if (PartitioningMethod.PARTITION_KEY_AS_PARTITION.equals(partitioningMethod)) {
      // Create all partition senders
      perStreamEventHubClientManagers.forEach((streamId, samzaEventHubClient) -> {
          EventHubClient ehClient = samzaEventHubClient.getEventHubClient();

          try {
            Map<Integer, PartitionSender> partitionSenders = new HashMap<>();
            long timeoutMs = config.getRuntimeInfoWaitTimeMS(systemName);
            Integer numPartitions =
                ehClient.getRuntimeInformation().get(timeoutMs, TimeUnit.MILLISECONDS).getPartitionCount();

            for (int i = 0; i < numPartitions; i++) {
              String partitionId = String.valueOf(i);
              EventHubClientManager perPartitionClientManager =
                  createOrGetEventHubClientManagerForPartition(streamId, i);
              PartitionSender partitionSender =
                  perPartitionClientManager.getEventHubClient().createPartitionSenderSync(partitionId);
              partitionSenders.put(i, partitionSender);
            }

            streamPartitionSenders.put(streamId, partitionSenders);
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            String msg = "Failed to fetch number of Event Hub partitions for partition sender creation";
            throw new SamzaException(msg, e);
          } catch (EventHubException | IllegalArgumentException e) {
            String msg = "Creation of partition sender failed with exception";
            throw new SamzaException(msg, e);
          }
        });
    }

    // Initiate metrics
    streamIds.forEach((streamId) -> {
        eventSkipRate.put(streamId, metricsRegistry.newCounter(streamId, EVENT_SKIP_RATE));
        eventWriteRate.put(streamId, metricsRegistry.newCounter(streamId, EVENT_WRITE_RATE));
        eventByteWriteRate.put(streamId, metricsRegistry.newCounter(streamId, EVENT_BYTE_WRITE_RATE));
      });

    // Locking to ensure that these aggregated metrics will be created only once across multiple system producers.
    synchronized (AGGREGATE_METRICS_LOCK) {
      if (aggEventWriteRate == null) {
        aggEventSkipRate = metricsRegistry.newCounter(AGGREGATE, EVENT_SKIP_RATE);
        aggEventWriteRate = metricsRegistry.newCounter(AGGREGATE, EVENT_WRITE_RATE);
        aggEventByteWriteRate = metricsRegistry.newCounter(AGGREGATE, EVENT_BYTE_WRITE_RATE);
      }
    }

    isStarted = true;
  }

  @Override
  public synchronized void flush(String source) {
    super.flush(source);
  }

  @Override
  public synchronized CompletableFuture<Void> sendAsync(String source, OutgoingMessageEnvelope envelope) {
    LOG.debug(String.format("Trying to send %s", envelope));
    if (!isStarted) {
      throw new SamzaException("Trying to call send before the producer is started.");
    }

    String streamId = config.getStreamId(envelope.getSystemStream().getStream());

    if (!perStreamEventHubClientManagers.containsKey(streamId)) {
      String msg = String.format("Trying to send event to a destination {%s} that is not registered.", streamId);
      throw new SamzaException(msg);
    }

    EventData eventData = createEventData(streamId, envelope);
    // SAMZA-1654: waiting for the client library to expose the API to calculate the exact size of the AMQP message
    // https://github.com/Azure/azure-event-hubs-java/issues/305
    int eventDataLength = eventData.getBytes() == null ? 0 : eventData.getBytes().length;

    // If the maxMessageSize is lesser than zero, then it means there is no message size restriction.
    if (this.maxMessageSize > 0 && eventDataLength > this.maxMessageSize) {
      LOG.info("Received a message with size {} > maxMessageSize configured {(}), Skipping it", eventDataLength,
          this.maxMessageSize);
      eventSkipRate.get(streamId).inc();
      aggEventSkipRate.inc();
      return CompletableFuture.completedFuture(null);
    }

    eventWriteRate.get(streamId).inc();
    aggEventWriteRate.inc();
    eventByteWriteRate.get(streamId).inc(eventDataLength);
    aggEventByteWriteRate.inc(eventDataLength);
    EventHubClientManager ehClient = perStreamEventHubClientManagers.get(streamId);

    // Async send call
    return sendToEventHub(streamId, eventData, getEnvelopePartitionId(envelope), ehClient.getEventHubClient());
  }

  private CompletableFuture<Void> sendToEventHub(String streamId, EventData eventData, Object partitionKey,
      EventHubClient eventHubClient) {
    if (PartitioningMethod.ROUND_ROBIN.equals(partitioningMethod)) {
      return eventHubClient.send(eventData);
    } else if (PartitioningMethod.EVENT_HUB_HASHING.equals(partitioningMethod)) {
      if (partitionKey == null) {
        throw new SamzaException("Partition key cannot be null for EventHub hashing");
      }
      return eventHubClient.send(eventData, convertPartitionKeyToString(partitionKey));
    } else if (PartitioningMethod.PARTITION_KEY_AS_PARTITION.equals(partitioningMethod)) {
      if (!(partitionKey instanceof Integer)) {
        String msg = "Partition key should be of type Integer";
        throw new SamzaException(msg);
      }

      Integer numPartition = streamPartitionSenders.get(streamId).size();
      Integer destinationPartition = (Integer) partitionKey % numPartition;

      PartitionSender sender = streamPartitionSenders.get(streamId).get(destinationPartition);
      return sender.send(eventData);
    } else {
      throw new SamzaException("Unknown partitioning method " + partitioningMethod);
    }
  }

  protected Object getEnvelopePartitionId(OutgoingMessageEnvelope envelope) {
    return envelope.getPartitionKey() == null ? envelope.getKey() : envelope.getPartitionKey();
  }

  private String convertPartitionKeyToString(Object partitionKey) {
    String partitionKeyStr;
    if (partitionKey instanceof String) {
      partitionKeyStr = (String) partitionKey;
    } else if (partitionKey instanceof Integer) {
      partitionKeyStr = String.valueOf(partitionKey);
    } else if (partitionKey instanceof byte[]) {
      partitionKeyStr = new String((byte[]) partitionKey, Charset.defaultCharset());
    } else {
      throw new SamzaException("Unsupported key type: " + partitionKey.getClass().toString());
    }
    if (partitionKeyStr != null && partitionKeyStr.length() > ClientConstants.MAX_PARTITION_KEY_LENGTH) {
      LOG.debug("Length of partition key: {} exceeds limit: {}. Truncating.", partitionKeyStr.length(),
          ClientConstants.MAX_PARTITION_KEY_LENGTH);
      partitionKeyStr = partitionKeyStr.substring(0, ClientConstants.MAX_PARTITION_KEY_LENGTH);
    }
    return partitionKeyStr;
  }

  protected EventData createEventData(String streamId, OutgoingMessageEnvelope envelope) {
    Optional<Interceptor> interceptor = Optional.ofNullable(interceptors.getOrDefault(streamId, null));
    byte[] eventValue = (byte[]) envelope.getMessage();
    if (interceptor.isPresent()) {
      eventValue = interceptor.get().intercept(eventValue);
    }

    EventData eventData = new EventDataImpl(eventValue);

    eventData.getProperties().put(PRODUCE_TIMESTAMP, Long.toString(System.currentTimeMillis()));

    if (config.getSendKeyInEventProperties(systemName)) {
      String keyValue = "";
      if (envelope.getKey() != null) {
        keyValue = (envelope.getKey() instanceof byte[]) ? new String((byte[]) envelope.getKey())
            : envelope.getKey().toString();
      }
      eventData.getProperties().put(KEY, keyValue);
    }
    return eventData;
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping producer.");
    streamPartitionSenders.values().forEach((streamPartitionSender) -> {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        streamPartitionSender.forEach((key, value) -> futures.add(value.close()));
        CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        try {
          future.get(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
          LOG.error("Closing the partition sender failed ", e);
        }
      });
    perStreamEventHubClientManagers.values().forEach(ehClient -> ehClient.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
    perStreamEventHubClientManagers.clear();
    if (config.getPerPartitionConnection(systemName)) {
      perPartitionEventHubClients.values()
          .stream()
          .flatMap(map -> map.values().stream())
          .forEach(ehClient -> ehClient.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
      perPartitionEventHubClients.clear();
    }
  }

  /**
   * Gets pending futures. Used by the test.
   *
   * @return the pending futures
   */
  Collection<CompletableFuture<Void>> getPendingFutures() {
    return pendingFutures;
  }
}
