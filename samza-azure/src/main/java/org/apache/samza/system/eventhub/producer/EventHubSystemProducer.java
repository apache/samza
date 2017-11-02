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
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.eventhub.EventHubClientManager;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.EventHubClientManagerFactory;
import org.apache.samza.system.eventhub.Interceptor;
import org.apache.samza.system.eventhub.metrics.SamzaHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class EventHubSystemProducer implements SystemProducer {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemProducer.class.getName());
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();
  private static final long DEFAULT_FLUSH_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  public static final String PRODUCE_TIMESTAMP = "produce-timestamp";

  // Metrics recording
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
  private static final String AGGREGATE = "aggregate";

  private static final Object AGGREGATE_METRICS_LOCK = new Object();

  public enum PartitioningMethod {
    EVENT_HUB_HASHING,
    PARTITION_KEY_AS_PARTITION,
  }

  private final HashMap<String, Counter> eventWriteRate = new HashMap<>();
  private final HashMap<String, Counter> eventByteWriteRate = new HashMap<>();
  private final HashMap<String, SamzaHistogram> sendLatency = new HashMap<>();
  private final HashMap<String, SamzaHistogram> sendCallbackLatency = new HashMap<>();
  private final HashMap<String, Counter> sendErrors = new HashMap<>();

  private final EventHubClientManagerFactory eventHubClientManagerFactory;
  private final EventHubConfig config;
  private final MetricsRegistry registry;
  private final PartitioningMethod partitioningMethod;
  private final String systemName;

  private final AtomicReference<Throwable> sendExceptionOnCallback = new AtomicReference<>(null);
  private volatile boolean isStarted = false;

  // Map of the system name to the event hub client.
  private final Map<String, EventHubClientManager> eventHubClients = new HashMap<>();
  private final Map<String, Map<Integer, PartitionSender>> streamPartitionSenders = new HashMap<>();

  private final Map<String, Interceptor> interceptors;

  private final Set<CompletableFuture<Void>> pendingFutures = ConcurrentHashMap.newKeySet();

  public EventHubSystemProducer(EventHubConfig config, String systemName,
                                EventHubClientManagerFactory eventHubClientManagerFactory,
                                Map<String, Interceptor> interceptors, MetricsRegistry registry) {
    this.config = config;
    this.registry = registry;
    this.systemName = systemName;
    this.partitioningMethod = config.getPartitioningMethod(systemName);
    this.eventHubClientManagerFactory = eventHubClientManagerFactory;
    this.interceptors = interceptors;
  }

  @Override
  public synchronized void register(String streamName) {
    LOG.debug("Trying to register {}.", streamName);
    if (isStarted) {
      String msg = "Cannot register once the producer is started.";
      throw new SamzaException(msg);
    }

    if (eventHubClients.containsKey(streamName)) {
      LOG.warn("Already registered stream {}.", streamName);
      return;
    }

    EventHubClientManager ehClient = eventHubClientManagerFactory.getEventHubClientManager(systemName, streamName, config);

    ehClient.init();
    eventHubClients.put(streamName, ehClient);
  }

  @Override
  public synchronized void start() {
    LOG.debug("Starting system producer.");

    if (PartitioningMethod.PARTITION_KEY_AS_PARTITION.equals(partitioningMethod)) {
      // Create all partition senders
      eventHubClients.forEach((streamName, samzaEventHubClient) -> {
          EventHubClient ehClient = samzaEventHubClient.getEventHubClient();

          try {
            Map<Integer, PartitionSender> partitionSenders = new HashMap<>();
            long timeoutMs = config.getRuntimeInfoWaitTimeMS(systemName);
            Integer numPartitions = ehClient.getRuntimeInformation().get(timeoutMs, TimeUnit.MILLISECONDS)
                    .getPartitionCount();

            for (int i = 0; i < numPartitions; i++) { // 32 partitions max
              String partitionId = String.valueOf(i);
              PartitionSender partitionSender = ehClient.createPartitionSenderSync(partitionId);
              partitionSenders.put(i, partitionSender);
            }

            streamPartitionSenders.put(streamName, partitionSenders);
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            String msg = "Failed to fetch number of Event Hub partitions for partition sender creation";
            throw new SamzaException(msg, e);
          } catch (ServiceBusException | IllegalArgumentException e) {
            String msg = "Creation of partition sender failed with exception";
            throw new SamzaException(msg, e);
          }
        });
    }

    for (String eventHub : eventHubClients.keySet()) {
      eventWriteRate.put(eventHub, registry.newCounter(eventHub, EVENT_WRITE_RATE));
      eventByteWriteRate.put(eventHub, registry.newCounter(eventHub, EVENT_BYTE_WRITE_RATE));
      sendLatency.put(eventHub, new SamzaHistogram(registry, eventHub, SEND_LATENCY));
      sendCallbackLatency.put(eventHub, new SamzaHistogram(registry, eventHub, SEND_CALLBACK_LATENCY));
      sendErrors.put(eventHub, registry.newCounter(eventHub, SEND_ERRORS));
    }

    // Locking to ensure that these aggregated metrics will be created only once across multiple system consumers.
    synchronized (AGGREGATE_METRICS_LOCK) {
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
  public synchronized void send(String destination, OutgoingMessageEnvelope envelope) {
    if (!isStarted) {
      throw new SamzaException("Trying to call send before the producer is started.");
    }

    if (!eventHubClients.containsKey(destination)) {
      String msg = String.format("Trying to send event to a destination {%s} that is not registered.", destination);
      throw new SamzaException(msg);
    }

    checkCallbackThrowable("Received exception on message send");

    EventData eventData = createEventData(destination, envelope);
    int eventDataLength =  eventData.getBytes() == null ? 0 : eventData.getBytes().length;
    eventWriteRate.get(destination).inc();
    aggEventWriteRate.inc();
    eventByteWriteRate.get(destination).inc(eventDataLength);
    aggEventByteWriteRate.inc(eventDataLength);
    EventHubClientManager ehClient = eventHubClients.get(destination);

    long beforeSendTimeMs = System.currentTimeMillis();

    // Async send call
    CompletableFuture<Void> sendResult = sendToEventHub(destination, eventData, getEnvelopePartitionId(envelope),
            ehClient.getEventHubClient());

    long afterSendTimeMs = System.currentTimeMillis();
    long latencyMs = afterSendTimeMs - beforeSendTimeMs;
    sendLatency.get(destination).update(latencyMs);
    aggSendLatency.update(latencyMs);

    pendingFutures.add(sendResult);

    // Auto update the metrics and possible throwable when futures are complete.
    sendResult.handle((aVoid, throwable) -> {
        long callbackLatencyMs = System.currentTimeMillis() - afterSendTimeMs;
        sendCallbackLatency.get(destination).update(callbackLatencyMs);
        aggSendCallbackLatency.update(callbackLatencyMs);
        if (throwable != null) {
          sendErrors.get(destination).inc();
          aggSendErrors.inc();
          LOG.error("Send message to event hub: {} failed with exception: ", destination, throwable);
          sendExceptionOnCallback.compareAndSet(null, throwable);
        }
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
        throw new SamzaException(msg);
      }

      Integer numPartition = streamPartitionSenders.get(streamName).size();
      Integer destinationPartition = (Integer) partitionKey % numPartition;

      PartitionSender sender = streamPartitionSenders.get(streamName).get(destinationPartition);
      return sender.send(eventData);
    } else {
      throw new SamzaException("Unknown partitioning method " + partitioningMethod);
    }
  }

  protected Object getEnvelopePartitionId(OutgoingMessageEnvelope envelope) {
    return envelope.getPartitionKey();
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

  private EventData createEventData(String streamName, OutgoingMessageEnvelope envelope) {
    Optional<Interceptor> interceptor = Optional.ofNullable(interceptors.getOrDefault(streamName, null));
    byte[] eventValue = (byte[]) envelope.getMessage();
    if (interceptor.isPresent()) {
      eventValue = interceptor.get().intercept(eventValue);
    }

    EventData eventData = new EventData(eventValue);

    eventData.getProperties().put(PRODUCE_TIMESTAMP, Long.toString(System.currentTimeMillis()));

    if (config.getSendKeyInEventProperties(systemName)) {
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
  public synchronized void flush(String source) {
    LOG.debug("Trying to flush pending {} sends messages.", pendingFutures.size());
    checkCallbackThrowable("Received exception on message send");

    CompletableFuture<Void> future = CompletableFuture
              .allOf(pendingFutures.toArray(new CompletableFuture[pendingFutures.size()]));

    try {
      // Block until all the pending sends are complete or timeout.
      future.get(DEFAULT_FLUSH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      String msg = "Flush failed with error";
      throw new SamzaException(msg, e);
    }

    checkCallbackThrowable("Sending one or more of the messages failed during flush");
  }

  private void checkCallbackThrowable(String msg) {
    // Check for send errors from EventHub side
    Throwable sendThrowable = sendExceptionOnCallback.get();
    if (sendThrowable != null) {
      throw new SamzaException(msg, sendThrowable);
    }
    pendingFutures.clear();
  }

  @Override
  public synchronized void stop() {
    LOG.debug("Stopping producer.", pendingFutures.size());

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
    eventHubClients.values().forEach(ehClient -> ehClient.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
    eventHubClients.clear();
  }

  Collection<CompletableFuture<Void>> getPendingFutures() {
    return pendingFutures;
  }
}
