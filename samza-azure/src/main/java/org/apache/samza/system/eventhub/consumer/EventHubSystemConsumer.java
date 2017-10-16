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

package org.apache.samza.system.eventhub.consumer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.SamzaEventHubClientFactory;
import org.apache.samza.system.eventhub.SamzaEventHubClient;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.metrics.SamzaHistogram;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Implementation of a system consumer for EventHubs. For each system stream
 * partition, it registers a handler with the EventHubsClient which constantly
 * push data into a block queue. This class extends the BlockingEnvelopeMap
 * provided by samza-api to to simplify the logic around those blocking queues.
 * <p>
 * A high level architecture:
 * <p>
 * ┌───────────────────────────────────────────────┐
 * │ EventHubsClient                               │
 * │                                               │
 * │   ┌───────────────────────────────────────┐   │        ┌─────────────────────┐
 * │   │                                       │   │        │                     │
 * │   │       PartitionReceiveHandler_1       │───┼───────▶│ SSP1-BlockingQueue  ├──────┐
 * │   │                                       │   │        │                     │      │
 * │   └───────────────────────────────────────┘   │        └─────────────────────┘      │
 * │                                               │                                     │
 * │   ┌───────────────────────────────────────┐   │        ┌─────────────────────┐      │
 * │   │                                       │   │        │                     │      │
 * │   │       PartitionReceiveHandler_2       │───┼───────▶│ SSP2-BlockingQueue  ├──────┤        ┌──────────────────────────┐
 * │   │                                       │   │        │                     │      ├───────▶│                          │
 * │   └───────────────────────────────────────┘   │        └─────────────────────┘      └───────▶│  SystemConsumer.poll()   │
 * │                                               │                                     ┌───────▶│                          │
 * │                                               │                                     │        └──────────────────────────┘
 * │                      ...                      │                  ...                │
 * │                                               │                                     │
 * │                                               │                                     │
 * │   ┌───────────────────────────────────────┐   │        ┌─────────────────────┐      │
 * │   │                                       │   │        │                     │      │
 * │   │       PartitionReceiveHandler_N       │───┼───────▶│ SSPN-BlockingQueue  ├──────┘
 * │   │                                       │   │        │                     │
 * │   └───────────────────────────────────────┘   │        └─────────────────────┘
 * │                                               │
 * │                                               │
 * └───────────────────────────────────────────────┘
 */
public class EventHubSystemConsumer extends BlockingEnvelopeMap {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemConsumer.class);
  private static final int MAX_EVENT_COUNT_PER_PARTITION_POLL = 50;
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  public static final String START_OF_STREAM = PartitionReceiver.START_OF_STREAM; // -1
  public static final String END_OF_STREAM = "-2";
  public static final String AGGREGATE = "aggregate";
  public static final String EVENT_READ_RATE = "eventReadRate";
  public static final String EVENT_BYTE_READ_RATE = "eventByteReadRate";
  public static final String READ_LATENCY = "readLatency";
  public static final String READ_ERRORS = "readErrors";

  private static Counter aggEventReadRate = null;
  private static Counter aggEventByteReadRate = null;
  private static SamzaHistogram aggReadLatency = null;
  private static Counter aggReadErrors = null;

  private final Map<String, Counter> eventReadRates;
  private final Map<String, Counter> eventByteReadRates;
  private final Map<String, SamzaHistogram> readLatencies;
  private final Map<String, Counter> readErrors;

  final Map<SystemStreamPartition, PartitionReceiveHandler> streamPartitionHandlers = new HashMap<>();
  private final Map<SystemStreamPartition, PartitionReceiver> streamPartitionReceivers = new HashMap<>();
  private final Map<String, SamzaEventHubClient> streamEventHubClients = new HashMap<>();
  private final Map<SystemStreamPartition, String> streamPartitionStartingOffsets = new HashMap<>();
  private final Map<String, Serde<byte[]>> serdes;
  private boolean isStarted = false;
  private final EventHubConfig config;
  private final String systemName;


  public EventHubSystemConsumer(EventHubConfig config, String systemName,
                                SamzaEventHubClientFactory samzaEventHubClientFactory,
                                Map<String, Serde<byte[]>> serdes,  MetricsRegistry registry) {
    super(registry, System::currentTimeMillis);

    this.config = config;
    this.systemName = systemName;
    this.serdes = serdes;
    List<String> streamNames = config.getStreams(systemName);
    // Create and initiate connections to Event Hubs
    for (String streamName : streamNames) {
      SamzaEventHubClient ehClientWrapper = samzaEventHubClientFactory
              .getSamzaEventHubClient(systemName, streamName, config);
      streamEventHubClients.put(streamName, ehClientWrapper);
      ehClientWrapper.init();
    }

    // Initiate metrics
    eventReadRates = streamNames.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_READ_RATE)));
    eventByteReadRates = streamNames.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_BYTE_READ_RATE)));
    readLatencies = streamNames.stream()
            .collect(Collectors.toMap(Function.identity(), x -> new SamzaHistogram(registry, x, READ_LATENCY)));
    readErrors = streamNames.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, READ_ERRORS)));

    // Locking to ensure that these aggregated metrics will be created only once across multiple system consumers.
    synchronized (AGGREGATE) {
      if (aggEventReadRate == null) {
        aggEventReadRate = registry.newCounter(AGGREGATE, EVENT_READ_RATE);
        aggEventByteReadRate = registry.newCounter(AGGREGATE, EVENT_BYTE_READ_RATE);
        aggReadLatency = new SamzaHistogram(registry, AGGREGATE, READ_LATENCY);
        aggReadErrors = registry.newCounter(AGGREGATE, READ_ERRORS);
      }
    }
  }

  @Override
  public synchronized void register(SystemStreamPartition systemStreamPartition, String offset) {
    super.register(systemStreamPartition, offset);

    if (isStarted) {
      throw new SamzaException("Trying to add partition when the connection has already started.");
    }

    if (StringUtils.isBlank(offset)) {
      switch (config.getStartPosition(systemName, systemStreamPartition.getStream())) {
        case EARLIEST:
          offset = START_OF_STREAM;
          break;
        case LATEST:
          offset = END_OF_STREAM;
          break;
        default:
          throw new SamzaException("Unknown starting position config " +
                  config.getStartPosition(systemName, systemStreamPartition.getStream()));
      }
    }
    streamPartitionStartingOffsets.put(systemStreamPartition, offset);
  }

  @Override
  public synchronized void start() {
    isStarted = true;
    // Create receivers for Event Hubs
    for (Map.Entry<SystemStreamPartition, String> entry : streamPartitionStartingOffsets.entrySet()) {
      SystemStreamPartition ssp = entry.getKey();
      String streamName = ssp.getStream();
      Integer partitionId = ssp.getPartition().getPartitionId();
      String offset = entry.getValue();
      String consumerGroup = config.getStreamConsumerGroup(systemName, streamName);
      String namespace = config.getStreamNamespace(systemName, streamName);
      String entityPath = config.getStreamEntityPath(systemName, streamName);
      SamzaEventHubClient ehClientWrapper = streamEventHubClients.get(streamName);
      try {
        PartitionReceiver receiver;
        if (offset.equals(EventHubSystemConsumer.END_OF_STREAM)) {
          receiver = ehClientWrapper.getEventHubClient()
                  .createReceiverSync(consumerGroup, partitionId.toString(), Instant.now());
        } else {
          receiver = ehClientWrapper.getEventHubClient()
                  .createReceiverSync(consumerGroup, partitionId.toString(), offset,
                          !offset.equals(EventHubSystemConsumer.START_OF_STREAM));
        }
        PartitionReceiveHandler handler = new PartitionReceiverHandlerImpl(ssp, eventReadRates.get(streamName),
                eventByteReadRates.get(streamName), readLatencies.get(streamName), readErrors.get(streamName),
                serdes.getOrDefault(streamName, null));
        streamPartitionHandlers.put(ssp, handler);

        receiver.setReceiveHandler(handler);
        streamPartitionReceivers.put(ssp, receiver);
      } catch (Exception e) {
        throw new SamzaException(
                String.format("Failed to create receiver for EventHubs: namespace=%s, entity=%s, partitionId=%d",
                        namespace, entityPath, partitionId), e);
      }
      LOG.info(String.format("Connection successfully started for namespace=%s, entity=%s ", namespace, entityPath));

    }
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping event hub system consumer...");
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    streamPartitionReceivers.values().forEach((receiver) -> futures.add(receiver.close()));
    CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    try {
      future.get(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.warn("Failed to close receivers", e);
    }
    streamEventHubClients.values().forEach(ehClient -> ehClient.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
  }

  @Override
  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
    return new LinkedBlockingQueue<>(config.getConsumerBufferCapacity(systemName));
  }

  protected class PartitionReceiverHandlerImpl extends PartitionReceiveHandler {

    private final Counter eventReadRate;
    private final Counter eventByteReadRate;
    private final SamzaHistogram readLatency;
    private final Counter errorRate;
    private final Serde<byte[]> serde;
    SystemStreamPartition ssp;

    PartitionReceiverHandlerImpl(SystemStreamPartition ssp, Counter eventReadRate, Counter eventByteReadRate,
                                 SamzaHistogram readLatency, Counter readErrors, Serde<byte[]> serde) {
      super(MAX_EVENT_COUNT_PER_PARTITION_POLL);
      this.ssp = ssp;
      this.eventReadRate = eventReadRate;
      this.eventByteReadRate = eventByteReadRate;
      this.readLatency = readLatency;
      errorRate = readErrors;
      this.serde = serde;
    }

    @Override
    public void onReceive(Iterable<EventData> events) {
      if (events != null) {

        events.forEach(event -> {
            byte[] decryptedBody = event.getBytes();
            if (serde != null) {
              decryptedBody = serde.fromBytes(decryptedBody);
            }
            try {
              updateMetrics(event);
              // note that the partition key can be null
              put(ssp, new EventHubIME(ssp, event.getSystemProperties().getOffset(),
                      event.getSystemProperties().getPartitionKey(), decryptedBody, event));
            } catch (Exception e) {
              String msg = String.format("Exception while adding the event from ssp %s to dispatch queue.", ssp);
              LOG.error(msg, e);
              throw new SamzaException(msg, e);
            }
          });
      }
    }

    private void updateMetrics(EventData event) {
      int eventDataLength = event.getBytes() == null ? 0 : event.getBytes().length;
      eventReadRate.inc();
      aggEventReadRate.inc();
      eventByteReadRate.inc(eventDataLength);
      aggEventByteReadRate.inc(eventDataLength);
      long latencyMs = Duration.between(Instant.now(), event.getSystemProperties().getEnqueuedTime()).toMillis();
      readLatency.update(latencyMs);
      aggReadLatency.update(latencyMs);
    }

    @Override
    public void onError(Throwable throwable) {
      errorRate.inc();
      aggReadErrors.inc();
      LOG.error(String.format("Received error from event hub connection (ssp=%s): ", ssp), throwable);
    }
  }
}
