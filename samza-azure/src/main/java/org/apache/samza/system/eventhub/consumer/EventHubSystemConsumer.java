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
import com.microsoft.azure.servicebus.StringUtil;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventDataWrapper;
import org.apache.samza.system.eventhub.EventHubClientWrapperFactory;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
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
import java.util.concurrent.*;
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
  private static final int MAX_EVENT_COUNT_PER_PARTITION_POLL = 50; //TODO

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

  private Map<String, Counter> eventReadRates;
  private Map<String, Counter> eventByteReadRates;
  private Map<String, SamzaHistogram> readLatencies;
  private Map<String, Counter> readErrors;

  final Map<SystemStreamPartition, PartitionReceiveHandler> streamPartitionHandlers = new HashMap<>();
  private final Map<SystemStreamPartition, PartitionReceiver> streamPartitionReceivers = new HashMap<>();
  private final Map<String, EventHubClientWrapper> streamEventHubClients = new HashMap<>();
  private final Map<SystemStreamPartition, String> streamPartitionStartingOffsets = new HashMap<>();
  private Map<String, Serde<byte[]>> serdes;
  private boolean isStarted = false;
  private final EventHubConfig config;

  public EventHubSystemConsumer(EventHubConfig config, EventHubClientWrapperFactory eventHubClientWrapperFactory,
                                Map<String, Serde<byte[]>> serdes,  MetricsRegistry registry) {
    super(registry, System::currentTimeMillis);

    this.config = config;
    this.serdes = serdes;
    List<String> streamList = config.getStreamList();

    // Create and initiate connections to Event Hubs
    for (String streamName : streamList) {
      String namespace = config.getStreamNamespace(streamName);
      String entityPath = config.getStreamEntityPath(streamName);
      String sasKeyName = config.getStreamSasKeyName(streamName);
      String sasKey = config.getStreamSasToken(streamName);
      LOG.info(String.format("Starting connection for namespace=%s, entity=%s ", namespace, entityPath));
      EventHubClientWrapper ehClientWrapper = eventHubClientWrapperFactory
              .getEventHubClientWrapper(namespace, entityPath, sasKeyName, sasKey, config);
      streamEventHubClients.put(streamName, ehClientWrapper);
      ehClientWrapper.init();
    }

    // Initiate metrics
    eventReadRates = streamList.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_READ_RATE)));
    eventByteReadRates = streamList.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_BYTE_READ_RATE)));
    readLatencies = streamList.stream()
            .collect(Collectors.toMap(Function.identity(), x -> new SamzaHistogram(registry, x, READ_LATENCY)));
    readErrors = streamList.stream()
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

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void start() {
    isStarted = true;
    // Create receivers for Event Hubs
    for (Map.Entry<SystemStreamPartition, String> entry : streamPartitionStartingOffsets.entrySet()) {
      SystemStreamPartition ssp = entry.getKey();
      String streamName = ssp.getStream();
      Integer partitionId = ssp.getPartition().getPartitionId();
      String offset = entry.getValue();
      String consumerGroup = config.getStreamConsumerGroup(streamName);
      String namespace = config.getStreamNamespace(streamName);
      String entityPath = config.getStreamEntityPath(streamName);
      EventHubClientWrapper ehClientWrapper = streamEventHubClients.get(streamName);
      try {
        PartitionReceiver receiver;
        if (StringUtil.isNullOrWhiteSpace(offset)) {
          throw new SamzaException(
                  String.format("Invalid offset %s system=%s, stream=%s", offset, namespace, entityPath));
        }
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

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void stop() {
    LOG.info("Stopping event hub system consumer...");
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    streamPartitionReceivers.values().forEach((receiver) -> futures.add(receiver.close()));
    CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    try {
      future.get(config.getShutdownWaitTimeMS(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      LOG.warn("Failed to close receivers", e);
    } catch (TimeoutException e) {
      LOG.warn("Closing the partition sender timed out ", e);
    }
    streamEventHubClients.values().forEach(ehClient -> ehClient.close(config.getShutdownWaitTimeMS()));
    streamEventHubClients.clear();
    streamPartitionStartingOffsets.clear();
    streamPartitionReceivers.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void register(SystemStreamPartition systemStreamPartition, String offset) {
    super.register(systemStreamPartition, offset);

    if (isStarted) {
      LOG.warn("Trying to add partition when the connection has already started.");
      return;
    }

    if (StringUtil.isNullOrWhiteSpace(offset)) {
      switch (config.getStartPosition(systemStreamPartition.getStream())) {
        case EARLIEST:
          offset = START_OF_STREAM;
          break;
        case LATEST:
          offset = END_OF_STREAM;
          break;
        default:
          throw new SamzaException(
                  "Unknown starting position config " + config.getStartPosition(systemStreamPartition.getStream()));
      }
    }
    streamPartitionStartingOffsets.put(systemStreamPartition, offset);
  }

  @Override
  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
    return new LinkedBlockingQueue<>(config.getConsumerBufferCapacity());
  }

  private class PartitionReceiverHandlerImpl extends PartitionReceiveHandler {

    private final Counter eventReadRate;
    private final Counter eventByteReadRate;
    private final SamzaHistogram readLatency;
    private final Counter errors;
    private final Serde<byte[]> serde;
    SystemStreamPartition ssp;

    PartitionReceiverHandlerImpl(SystemStreamPartition ssp, Counter eventReadRate, Counter eventByteReadRate,
                                 SamzaHistogram readLatency, Counter readErrors, Serde<byte[]> serde) {
      super(MAX_EVENT_COUNT_PER_PARTITION_POLL);
      this.ssp = ssp;
      this.eventReadRate = eventReadRate;
      this.eventByteReadRate = eventByteReadRate;
      this.readLatency = readLatency;
      errors = readErrors;
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
            EventDataWrapper wrappedEvent = new EventDataWrapper(event, decryptedBody);
            try {
              updateMetrics(event);
              // note that the partition key can be null
              put(ssp, new IncomingMessageEnvelope(ssp, event.getSystemProperties().getOffset(),
                      event.getSystemProperties().getPartitionKey(), wrappedEvent));
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
      errors.inc();
      aggReadErrors.inc();
      LOG.error(String.format("Received error from event hub connection (ssp=%s): ", ssp), throwable);
    }
  }
}
