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
import com.microsoft.azure.eventhubs.EventHubPartitionRuntimeInformation;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventHubClientManagerFactory;
import org.apache.samza.system.eventhub.EventHubClientManager;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.Interceptor;
import org.apache.samza.system.eventhub.admin.EventHubSystemAdmin;
import org.apache.samza.system.eventhub.metrics.SamzaHistogram;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

  // Overall timeout for EventHubClient exponential backoff policy
  private static final Duration DEFAULT_EVENTHUB_RECEIVER_TIMEOUT = Duration.ofMinutes(10L);
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  public static final String START_OF_STREAM = PartitionReceiver.START_OF_STREAM; // -1
  public static final String END_OF_STREAM = "-2";
  public static final String EVENT_READ_RATE = "eventReadRate";
  public static final String EVENT_BYTE_READ_RATE = "eventByteReadRate";
  public static final String READ_LATENCY = "readLatency";
  public static final String READ_ERRORS = "readErrors";
  public static final String AGGREGATE = "aggregate";

  private static final Object AGGREGATE_METRICS_LOCK = new Object();

  private static Counter aggEventReadRate = null;
  private static Counter aggEventByteReadRate = null;
  private static SamzaHistogram aggReadLatency = null;
  private static Counter aggReadErrors = null;

  private final Map<String, Counter> eventReadRates;
  private final Map<String, Counter> eventByteReadRates;
  private final Map<String, SamzaHistogram> readLatencies;
  private final Map<String, Counter> readErrors;

  final ConcurrentHashMap<SystemStreamPartition, PartitionReceiveHandler> streamPartitionHandlers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<SystemStreamPartition, PartitionReceiver> streamPartitionReceivers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, EventHubClientManager> streamEventHubManagers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<SystemStreamPartition, String> streamPartitionOffsets = new ConcurrentHashMap<>();
  private final Map<String, Interceptor> interceptors;
  private boolean isStarted = false;
  private final EventHubConfig config;
  private final String systemName;

  // Partition receiver error propagation
  private final AtomicReference<Throwable> eventHubHandlerError = new AtomicReference<>(null);

  public EventHubSystemConsumer(EventHubConfig config, String systemName,
                                EventHubClientManagerFactory eventHubClientManagerFactory,
                                Map<String, Interceptor> interceptors, MetricsRegistry registry) {
    super(registry, System::currentTimeMillis);

    this.config = config;
    this.systemName = systemName;
    this.interceptors = interceptors;
    List<String> streamIds = config.getStreams(systemName);
    // Create and initiate connections to Event Hubs
    for (String streamId : streamIds) {
      EventHubClientManager eventHubClientManager = eventHubClientManagerFactory
              .getEventHubClientManager(systemName, streamId, config);
      streamEventHubManagers.put(streamId, eventHubClientManager);
      eventHubClientManager.init();
    }

    // Initiate metrics
    eventReadRates = streamIds.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_READ_RATE)));
    eventByteReadRates = streamIds.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_BYTE_READ_RATE)));
    readLatencies = streamIds.stream()
            .collect(Collectors.toMap(Function.identity(), x -> new SamzaHistogram(registry, x, READ_LATENCY)));
    readErrors = streamIds.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, READ_ERRORS)));

    // Locking to ensure that these aggregated metrics will be created only once across multiple system consumers.
    synchronized (AGGREGATE_METRICS_LOCK) {
      if (aggEventReadRate == null) {
        aggEventReadRate = registry.newCounter(AGGREGATE, EVENT_READ_RATE);
        aggEventByteReadRate = registry.newCounter(AGGREGATE, EVENT_BYTE_READ_RATE);
        aggReadLatency = new SamzaHistogram(registry, AGGREGATE, READ_LATENCY);
        aggReadErrors = registry.newCounter(AGGREGATE, READ_ERRORS);
      }
    }
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    super.register(systemStreamPartition, offset);

    LOG.info(String.format("Eventhub consumer trying to register ssp %s, offset %s", systemStreamPartition, offset));
    if (isStarted) {
      throw new SamzaException("Trying to add partition when the connection has already started.");
    }

    if (streamPartitionOffsets.containsKey(systemStreamPartition)) {
      // Only update if new offset is lower than previous offset
      if (END_OF_STREAM.equals(offset)) return;
      String prevOffset = streamPartitionOffsets.get(systemStreamPartition);
      if (!END_OF_STREAM.equals(prevOffset) && EventHubSystemAdmin.compareOffsets(offset, prevOffset) > -1) {
        return;
      }
    }
    streamPartitionOffsets.put(systemStreamPartition, offset);
  }

  private String getNewestEventHubOffset(EventHubClientManager eventHubClientManager, String streamName, Integer partitionId) {
    CompletableFuture<EventHubPartitionRuntimeInformation> partitionRuntimeInfoFuture = eventHubClientManager
            .getEventHubClient()
            .getPartitionRuntimeInformation(partitionId.toString());
    try {
      long timeoutMs = config.getRuntimeInfoWaitTimeMS(systemName);

      EventHubPartitionRuntimeInformation partitionRuntimeInformation = partitionRuntimeInfoFuture
              .get(timeoutMs, TimeUnit.MILLISECONDS);

      return partitionRuntimeInformation.getLastEnqueuedOffset();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      String msg = String.format(
              "Error while fetching EventHubPartitionRuntimeInfo for System:%s, Stream:%s, Partition:%s",
              systemName, streamName, partitionId);
      throw new SamzaException(msg);
    }
  }

  @Override
  public void start() {
    isStarted = true;
    // Create receivers for Event Hubs
    for (Map.Entry<SystemStreamPartition, String> entry : streamPartitionOffsets.entrySet()) {

      SystemStreamPartition ssp = entry.getKey();
      String streamName = ssp.getStream();
      String streamId = config.getStreamId(ssp.getStream());
      Integer partitionId = ssp.getPartition().getPartitionId();
      String offset = entry.getValue();
      String consumerGroup = config.getStreamConsumerGroup(systemName, streamId);
      String namespace = config.getStreamNamespace(systemName, streamId);
      String entityPath = config.getStreamEntityPath(systemName, streamId);
      EventHubClientManager eventHubClientManager = streamEventHubManagers.get(streamId);

      try {
        // Fetch the newest offset
        String newestEventHubOffset = getNewestEventHubOffset(eventHubClientManager, streamName, partitionId);
        PartitionReceiver receiver;
        if (END_OF_STREAM.equals(offset) || EventHubSystemAdmin.compareOffsets(newestEventHubOffset, offset) == -1) {
          // If the offset is greater than the newest offset, use the use current Instant as
          // offset to fetch in Eventhub.
          receiver = eventHubClientManager.getEventHubClient()
                  .createReceiverSync(consumerGroup, partitionId.toString(), Instant.now());
        } else {
          // If the offset is less or equal to the newest offset in the system, it can be
          // used as the starting offset to receive from. EventHub will return the first
          // message AFTER the offset that was specified in the fetch request.
          // If no such offset exists Eventhub will return an error.
          receiver = eventHubClientManager.getEventHubClient()
                  .createReceiverSync(consumerGroup, partitionId.toString(), offset,
                          !offset.equals(EventHubSystemConsumer.START_OF_STREAM));
        }

        PartitionReceiveHandler handler = new PartitionReceiverHandlerImpl(ssp, eventReadRates.get(streamId),
                eventByteReadRates.get(streamId), readLatencies.get(streamId), readErrors.get(streamId),
                interceptors.getOrDefault(streamId, null));


        // Timeout for EventHubClient receive
        receiver.setReceiveTimeout(DEFAULT_EVENTHUB_RECEIVER_TIMEOUT);

        // Start the receiver thread
        receiver.setReceiveHandler(handler);

        streamPartitionHandlers.put(ssp, handler);
        streamPartitionReceivers.put(ssp, receiver);
      } catch (Exception e) {
        throw new SamzaException(
                String.format("Failed to create receiver for EventHubs: namespace=%s, entity=%s, partitionId=%d",
                        namespace, entityPath, partitionId), e);
      }
      LOG.debug(String.format("Connection successfully started for namespace=%s, entity=%s ", namespace, entityPath));

    }
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
    Throwable handlerError = eventHubHandlerError.get();

    if (handlerError != null) {
      if (isErrorTransient(handlerError)) {
        // Log a warning if the error is transient
        // Partition receiver handler OnError should have handled it by recreating the receiver
        LOG.warn("Received a transient error from event hub partition receiver, restarted receiver", handlerError);
      } else {
        // Propagate the error to user if the throwable is either
        // 1. permanent ServiceBusException error from client
        // 2. SamzaException thrown bu the EventHubConsumer
        //   2a. Interrupted during put operation to BEM
        //   2b. Failure in renewing the Partititon Receiver
        String msg = "Received a non transient error from event hub partition receiver";
        throw new SamzaException(msg, handlerError);
      }
    }

    return super.poll(systemStreamPartitions, timeout);
  }

  private void renewPartitionReceiver(SystemStreamPartition ssp) {
    String streamId = config.getStreamId(ssp.getStream());
    EventHubClientManager eventHubClientManager = streamEventHubManagers.get(streamId);
    String offset = streamPartitionOffsets.get(ssp);
    Integer partitionId = ssp.getPartition().getPartitionId();
    String consumerGroup = config.getStreamConsumerGroup(ssp.getSystem(), streamId);

    try {
      // Close current receiver
      streamPartitionReceivers.get(ssp).close().get(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

      // Recreate receiver
      PartitionReceiver receiver = eventHubClientManager.getEventHubClient()
              .createReceiverSync(consumerGroup, partitionId.toString(), offset,
                      !offset.equals(EventHubSystemConsumer.START_OF_STREAM));

      // Timeout for EventHubClient receive
      receiver.setReceiveTimeout(DEFAULT_EVENTHUB_RECEIVER_TIMEOUT);

      // Create and start receiver thread with handler
      receiver.setReceiveHandler(streamPartitionHandlers.get(ssp));
      streamPartitionReceivers.put(ssp, receiver);

    } catch (Exception e) {
      eventHubHandlerError.set(new SamzaException(
              String.format("Failed to recreate receiver for EventHubs after ReceiverHandlerError (ssp=%s)", ssp), e));
    }
  }

  @Override
  public void stop() {
    LOG.debug("Stopping event hub system consumer...");
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    streamPartitionReceivers.values().forEach((receiver) -> futures.add(receiver.close()));
    CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    try {
      future.get(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.warn("Failed to close receivers", e);
    }
    streamEventHubManagers.values().forEach(ehClientManager -> ehClientManager.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
  }

  private boolean isErrorTransient(Throwable throwable) {
    if (throwable instanceof ServiceBusException) {
      ServiceBusException serviceBusException = (ServiceBusException) throwable;
      return serviceBusException.getIsTransient();
    }
    return false;
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
    private final Interceptor interceptor;
    SystemStreamPartition ssp;

    PartitionReceiverHandlerImpl(SystemStreamPartition ssp, Counter eventReadRate, Counter eventByteReadRate,
                                 SamzaHistogram readLatency, Counter readErrors, Interceptor interceptor) {
      super(MAX_EVENT_COUNT_PER_PARTITION_POLL);
      this.ssp = ssp;
      this.eventReadRate = eventReadRate;
      this.eventByteReadRate = eventByteReadRate;
      this.readLatency = readLatency;
      this.errorRate = readErrors;
      this.interceptor = interceptor;
    }

    @Override
    public void onReceive(Iterable<EventData> events) {
      if (events != null) {
        events.forEach(event -> {
            byte[] eventDataBody = event.getBytes();
            if (interceptor != null) {
              eventDataBody = interceptor.intercept(eventDataBody);
            }
            String offset = event.getSystemProperties().getOffset();
            Object partitionKey = event.getSystemProperties().getPartitionKey();
            if (partitionKey == null) {
              partitionKey = event.getProperties().get(EventHubSystemProducer.KEY);
            }
            try {
              updateMetrics(event);

              // note that the partition key can be null
              put(ssp, new EventHubIncomingMessageEnvelope(ssp, offset, partitionKey, eventDataBody, event));
            } catch (InterruptedException e) {
              String msg = String.format("Interrupted while adding the event from ssp %s to dispatch queue.", ssp);
              LOG.error(msg, e);
              throw new SamzaException(msg, e);
            }

            // Cache latest checkpoint
            streamPartitionOffsets.put(ssp, offset);
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

      if (throwable instanceof ServiceBusException) {
        ServiceBusException busException = (ServiceBusException) throwable;

        if (busException.getIsTransient()) {

          // Only set to transient throwable if there has been no previous errors
          eventHubHandlerError.compareAndSet(null, throwable);

          // Retry creating a receiver since error likely due to timeout
          renewPartitionReceiver(ssp);
          return;
        }
      }

      // Propagate non transient or unknown errors
      eventHubHandlerError.set(throwable);
    }
  }
}
