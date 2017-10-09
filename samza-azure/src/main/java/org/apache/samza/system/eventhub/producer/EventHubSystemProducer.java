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

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.EventHubClientWrapperFactory;
import org.apache.samza.system.eventhub.metrics.SamzaHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class EventHubSystemProducer implements SystemProducer {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemProducer.class.getName());
  private static final long FLUSH_SLEEP_TIME_MILLIS = 1000;

  public static final String PRODUCE_TIMESTAMP = "produce-timestamp";

  // Metrics recording
  public static final String AGGREGATE = "aggregate";
  private static final String EVENT_WRITE_RATE = "eventWriteRate";
  private static final String EVENT_BYTE_WRITE_RATE = "eventByteWriteRate";
  private static final String SEND_ERRORS = "sendErrors";
  private static final String SEND_LATENCY = "sendLatency";
  private static final String SEND_CALLBACK_LATENCY = "sendCallbackLatency";
  private static Counter aggEventWriteRate = null;
  private static Counter aggEventByteWriteRate = null;
  private static Counter aggSendErrors = null;
  private static SamzaHistogram aggSendLatency = null;
  private static SamzaHistogram aggSendCallbackLatency = null;
  private HashMap<String, Counter> eventWriteRate = new HashMap<>();
  private HashMap<String, Counter> eventByteWriteRate = new HashMap<>();
  private HashMap<String, SamzaHistogram> sendLatency = new HashMap<>();
  private HashMap<String, SamzaHistogram> sendCallbackLatency = new HashMap<>();
  private HashMap<String, Counter> sendErrors = new HashMap<>();

  private final EventHubClientWrapperFactory eventHubClientWrapperFactory;
  private final EventHubConfig config;
  private final MetricsRegistry registry;
  private final PartitioningMethod partitioningMethod;

  private Throwable sendExceptionOnCallback;
  private boolean isStarted;

  // Map of the system name to the event hub client.
  private Map<String, EventHubClientWrapper> eventHubClients = new HashMap<>();
  private Map<String, Map<Integer, PartitionSender>> streamPartitionSenders = new HashMap<>();
  private Map<String, Integer> streamPartitionCounts = new HashMap<>();

  // Running count for the next message Id
  private long messageId;
  private Map<String, Serde<byte[]>> serdes;

  private Map<Long, CompletableFuture<Void>> pendingFutures = new ConcurrentHashMap<>();

  public EventHubSystemProducer(EventHubConfig config, EventHubClientWrapperFactory eventHubClientWrapperFactory,
                                Map<String, Serde<byte[]>> serdes, MetricsRegistry registry) {
    messageId = 0;
    this.config = config;
    this.registry = registry;
    partitioningMethod = this.config.getPartitioningMethod();
    this.eventHubClientWrapperFactory = eventHubClientWrapperFactory;
    this.serdes = serdes;
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting system producer.");
    for (String eventHub : eventHubClients.keySet()) {
      eventWriteRate.put(eventHub, registry.newCounter(eventHub, EVENT_WRITE_RATE));
      eventByteWriteRate.put(eventHub, registry.newCounter(eventHub, EVENT_BYTE_WRITE_RATE));
      sendLatency.put(eventHub, new SamzaHistogram(registry, eventHub, SEND_LATENCY));
      sendCallbackLatency.put(eventHub, new SamzaHistogram(registry, eventHub, SEND_CALLBACK_LATENCY));
      sendErrors.put(eventHub, registry.newCounter(eventHub, SEND_ERRORS));
    }

    // Locking to ensure that these aggregated metrics will be created only once across multiple system consumers.
    synchronized (AGGREGATE) {
      if (aggEventWriteRate == null) {
        aggEventWriteRate = registry.newCounter(AGGREGATE, EVENT_WRITE_RATE);
        aggEventByteWriteRate = registry.newCounter(AGGREGATE, EVENT_BYTE_WRITE_RATE);
        aggSendLatency = new SamzaHistogram(registry, AGGREGATE, SEND_LATENCY);
        aggSendCallbackLatency = new SamzaHistogram(registry, AGGREGATE, SEND_CALLBACK_LATENCY);
        aggSendErrors = registry.newCounter(AGGREGATE, SEND_ERRORS);
      }
    }

    isStarted = true;
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping event hub system producer...");
    streamPartitionSenders.values().forEach((streamPartitionSender) -> {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        streamPartitionSender.forEach((key, value) -> futures.add(value.close()));
        CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        try {
          future.get(config.getShutdownWaitTimeMS(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
          throw new SamzaException("Closing the partition sender failed ", e);
        }
      });
    eventHubClients.values().forEach(ehClient -> ehClient.close(config.getShutdownWaitTimeMS()));
    eventHubClients.clear();
  }

  @Override
  public synchronized void register(String streamName) {
    LOG.info("Trying to register {}.", streamName);
    if (isStarted) {
      String msg = "Cannot register once the producer is started.";
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    EventHubClientWrapper ehClient = eventHubClientWrapperFactory
            .getEventHubClientWrapper(config.getStreamNamespace(streamName), config.getStreamEntityPath(streamName),
                    config.getStreamSasKeyName(streamName), config.getStreamSasToken(streamName), config);

    ehClient.init();
    eventHubClients.put(streamName, ehClient);
    streamPartitionSenders.put(streamName, new HashMap<>());
  }

  @Override
  public synchronized void send(String destination, OutgoingMessageEnvelope envelope) {
    if (!isStarted) {
      throw new SamzaException("Trying to call send before the producer is started.");
    }

    if (!eventHubClients.containsKey(destination)) {
      String msg = String.format("Trying to send event to a destination {%s} that is not registered.", destination);
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    if (sendExceptionOnCallback != null) {
      SamzaException e = new SamzaException(sendExceptionOnCallback);
      sendExceptionOnCallback = null;
      pendingFutures.clear();
      LOG.error("One of the previous sends failed.");
      throw e;
    }

    EventData eventData = createEventData(destination, envelope);
    int eventDataLength =  eventData.getBytes() == null ? 0 : eventData.getBytes().length;
    eventWriteRate.get(destination).inc();
    aggEventWriteRate.inc();
    eventByteWriteRate.get(destination).inc(eventDataLength);
    aggEventByteWriteRate.inc(eventDataLength);
    EventHubClientWrapper ehClient = eventHubClients.get(destination);

    Instant startTime = Instant.now();

    CompletableFuture<Void> sendResult = sendToEventHub(destination, eventData, envelope.getPartitionKey(),
            ehClient.getEventHubClient());

    Instant endTime = Instant.now();
    long latencyMs = Duration.between(startTime, endTime).toMillis();
    sendLatency.get(destination).update(latencyMs);
    aggSendLatency.update(latencyMs);

    long messageId = ++this.messageId;

    // Rotate the messageIds
    if (messageId == Long.MAX_VALUE) {
      this.messageId = 0;
    }

    pendingFutures.put(messageId, sendResult);

    // Auto remove the future from the list when they are complete.
    sendResult.handle((aVoid, throwable) -> {
        long callbackLatencyMs = Duration.between(endTime, Instant.now()).toMillis();
        sendCallbackLatency.get(destination).update(callbackLatencyMs);
        aggSendCallbackLatency.update(callbackLatencyMs);
        if (throwable != null) {
          sendErrors.get(destination).inc();
          aggSendErrors.inc();
          LOG.error("Send message to event hub: {} failed with exception: ", destination, throwable);
          sendExceptionOnCallback = throwable;
        }
        pendingFutures.remove(messageId);
        return aVoid;
      });
  }

  private CompletableFuture<Void> sendToEventHub(String streamName, EventData eventData, Object partitionKey,
                                                 EventHubClient eventHubClient) {
    if (partitioningMethod == PartitioningMethod.EVENT_HUB_HASHING) {
      return eventHubClient.send(eventData, convertPartitionKeyToString(partitionKey));
    } else if (partitioningMethod == PartitioningMethod.PARTITION_KEY_AS_PARTITION) {
      if (!(partitionKey instanceof Integer)) {
        String msg = "Partition key should be of type Integer";
        LOG.error(msg);
        throw new SamzaException(msg);
      }

      PartitionSender sender = getPartitionSender(streamName, (int) partitionKey, eventHubClient);
      return sender.send(eventData);
    } else {
      throw new SamzaException("Unknown partitioning method " + partitioningMethod);
    }
  }

  private String convertPartitionKeyToString(Object partitionKey) {
    if (partitionKey instanceof String) {
      return (String) partitionKey;
    } else if (partitionKey instanceof Integer) {
      return String.valueOf(partitionKey);
    } else if (partitionKey instanceof byte[]) {
      return new String((byte[]) partitionKey, Charset.defaultCharset());
    } else {
      throw new SamzaException("Unsupported key type: " + partitionKey.getClass().toString());
    }
  }

  private PartitionSender getPartitionSender(String streamName, int partition, EventHubClient eventHubClient) {
    Map<Integer, PartitionSender> partitionSenders = streamPartitionSenders.get(streamName);
    if (!partitionSenders.containsKey(partition)) {
      try {
        if (!streamPartitionCounts.containsKey(streamName)) {
          streamPartitionCounts.put(streamName, eventHubClient.getRuntimeInformation()
                  .get(config.getRuntimeInfoWaitTimeMS(), TimeUnit.MILLISECONDS).getPartitionCount());
        }
        PartitionSender partitionSender = eventHubClient
                .createPartitionSenderSync(String.valueOf(partition % streamPartitionCounts.get(streamName)));
        partitionSenders.put(partition, partitionSender);
      } catch (ServiceBusException e) {
        String msg = "Creation of partition sender failed with exception";
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        String msg = "Failed to fetch number of Event Hub partitions for partition sender creation";
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      }
    }

    return partitionSenders.get(partition);
  }

  private EventData createEventData(String streamName, OutgoingMessageEnvelope envelope) {
    Optional<Serde<byte[]>> serde = Optional.ofNullable(serdes.getOrDefault(streamName, null));
    byte[] eventValue = (byte[]) envelope.getMessage();
    if (serde.isPresent()) {
      eventValue = serde.get().toBytes(eventValue);
    }

    EventData eventData = new EventData(eventValue);

    eventData.getProperties().put(PRODUCE_TIMESTAMP, Long.toString(System.currentTimeMillis()));

    if (config.getSendKeyInEventProperties()) {
      String keyValue = "";
      if (envelope.getKey() != null) {
        keyValue = (envelope.getKey() instanceof byte[]) ? new String((byte[]) envelope.getKey())
                : envelope.getKey().toString();
      }
      eventData.getProperties().put("key", keyValue);
    }
    return eventData;
  }

  @Override
  public void flush(String source) {
    LOG.info("Trying to flush pending {} sends messages: {}", pendingFutures.size(), pendingFutures.keySet());
    // Wait till all the pending sends are complete.
    while (!pendingFutures.isEmpty()) {
      try {
        Thread.sleep(FLUSH_SLEEP_TIME_MILLIS);
      } catch (InterruptedException e) {
        String msg = "Flush failed with error";
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      }
    }

    if (sendExceptionOnCallback != null) {
      String msg = "Sending one of the message failed during flush";
      Throwable throwable = sendExceptionOnCallback;
      sendExceptionOnCallback = null;
      LOG.error(msg, throwable);
      throw new SamzaException(msg, throwable);
    }

    LOG.info("Flush succeeded.");
  }

  Collection<CompletableFuture<Void>> getPendingFutures() {
    return pendingFutures.values();
  }

  public enum PartitioningMethod {
    EVENT_HUB_HASHING,
    PARTITION_KEY_AS_PARTITION,
  }
}
