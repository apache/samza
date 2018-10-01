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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.impl.ClientConstants;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SlidingTimeWindowReservoir;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventHubClientManager;
import org.apache.samza.system.eventhub.EventHubClientManagerFactory;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.Interceptor;
import org.apache.samza.system.eventhub.admin.EventHubSystemAdmin;
import org.apache.samza.system.eventhub.metrics.SamzaHistogram;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.Clock;
import org.apache.samza.util.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  // Overall timeout for EventHubClient exponential backoff policy
  private static final Duration DEFAULT_EVENTHUB_RECEIVER_TIMEOUT = Duration.ofMinutes(10L);
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = Duration.ofSeconds(15).toMillis();

  public static final String START_OF_STREAM = ClientConstants.START_OF_STREAM; // -1
  public static final String END_OF_STREAM = "-2";
  public static final String EVENT_READ_RATE = "eventReadRate";
  public static final String EVENT_BYTE_READ_RATE = "eventByteReadRate";
  public static final String CONSUMPTION_LAG_MS = "consumptionLagMs";
  public static final String READ_ERRORS = "readErrors";
  public static final String AGGREGATE = "aggregate";

  private static final Object AGGREGATE_METRICS_LOCK = new Object();

  private static Counter aggEventReadRate = null;
  private static Counter aggEventByteReadRate = null;
  private static SamzaHistogram aggConsumptionLagMs = null;
  private static Counter aggReadErrors = null;

  private final Map<String, Counter> eventReadRates;
  private final Map<String, Counter> eventByteReadRates;
  private final Map<String, SamzaHistogram> consumptionLagMs;
  private final Map<String, Counter> readErrors;

  @VisibleForTesting
  final Map<SystemStreamPartition, PartitionReceiveHandler> streamPartitionHandlers = new ConcurrentHashMap<>();
  @VisibleForTesting
  final Map<SystemStreamPartition, EventHubClientManager> perPartitionEventHubManagers = new ConcurrentHashMap<>();

  private final Map<SystemStreamPartition, PartitionReceiver> streamPartitionReceivers = new ConcurrentHashMap<>();
  // should remain empty if PerPartitionConnection is true
  private final Map<String, EventHubClientManager> perStreamEventHubManagers = new ConcurrentHashMap<>();
  private final Map<SystemStreamPartition, String> streamPartitionOffsets = new ConcurrentHashMap<>();
  private final Map<String, Interceptor> interceptors;
  private final Integer prefetchCount;
  private volatile boolean isStarted = false;
  private final EventHubConfig config;
  private final String systemName;
  private final EventHubClientManagerFactory eventHubClientManagerFactory;

  // Partition receiver non transient error propagation
  private final AtomicReference<Throwable> eventHubNonTransientError = new AtomicReference<>(null);

  private final ExecutorService reconnectTaskRunner = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("EventHubs-Reconnect-Task").setDaemon(true).build());
  private long lastRetryTs = 0;

  private final Clock clock;
  @VisibleForTesting
  final SlidingTimeWindowReservoir recentRetryAttempts;
  @VisibleForTesting
  volatile Future reconnectTaskStatus = null;

  public EventHubSystemConsumer(EventHubConfig config, String systemName,
      EventHubClientManagerFactory eventHubClientManagerFactory, Map<String, Interceptor> interceptors,
      MetricsRegistry registry) {
    this(config, systemName, eventHubClientManagerFactory, interceptors, registry, System::currentTimeMillis);
  }

  EventHubSystemConsumer(EventHubConfig config, String systemName,
      EventHubClientManagerFactory eventHubClientManagerFactory, Map<String, Interceptor> interceptors,
      MetricsRegistry registry, Clock clock) {
    super(registry, clock);

    this.config = config;
    this.clock = clock;
    this.systemName = systemName;
    this.interceptors = interceptors;
    this.eventHubClientManagerFactory = eventHubClientManagerFactory;
    List<String> streamIds = config.getStreams(systemName);
    prefetchCount = config.getPrefetchCount(systemName);

    recentRetryAttempts = new SlidingTimeWindowReservoir(config.getRetryWindowMs(systemName), clock);

    // Initiate metrics
    eventReadRates =
        streamIds.stream().collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_READ_RATE)));
    eventByteReadRates = streamIds.stream()
        .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_BYTE_READ_RATE)));
    consumptionLagMs = streamIds.stream()
        .collect(Collectors.toMap(Function.identity(), x -> new SamzaHistogram(registry, x, CONSUMPTION_LAG_MS)));
    readErrors =
        streamIds.stream().collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, READ_ERRORS)));

    // Locking to ensure that these aggregated metrics will be created only once across multiple system consumers.
    synchronized (AGGREGATE_METRICS_LOCK) {
      if (aggEventReadRate == null) {
        aggEventReadRate = registry.newCounter(AGGREGATE, EVENT_READ_RATE);
        aggEventByteReadRate = registry.newCounter(AGGREGATE, EVENT_BYTE_READ_RATE);
        aggConsumptionLagMs = new SamzaHistogram(registry, AGGREGATE, CONSUMPTION_LAG_MS);
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
      if (END_OF_STREAM.equals(offset)) {
        return;
      }
      String prevOffset = streamPartitionOffsets.get(systemStreamPartition);
      if (!END_OF_STREAM.equals(prevOffset) && EventHubSystemAdmin.compareOffsets(offset, prevOffset) > -1) {
        return;
      }
    }
    streamPartitionOffsets.put(systemStreamPartition, offset);
  }

  // Based on the config PerPartitionConnection, create or get EventHubClientManager for the SSP
  // Note: this should be used only when starting up. After initialization, directly use perPartitionEventHubManagers
  // to obtain the corresponding EventHubClientManager
  private EventHubClientManager createOrGetEventHubClientManagerForSSP(String streamId, SystemStreamPartition ssp) {
    EventHubClientManager eventHubClientManager;
    if (config.getPerPartitionConnection(systemName)) {
      // will create one EventHub client per partition
      if (perPartitionEventHubManagers.containsKey(ssp)) {
        LOG.warn(String.format("Trying to create new EventHubClientManager for ssp=%s. But one already exists", ssp));
        eventHubClientManager = perPartitionEventHubManagers.get(ssp);
      } else {
        LOG.info("Creating EventHub client manager for SSP: " + ssp);
        eventHubClientManager = eventHubClientManagerFactory.getEventHubClientManager(systemName, streamId, config);
        eventHubClientManager.init();
        perPartitionEventHubManagers.put(ssp, eventHubClientManager);
      }
    } else {
      // will share one EventHub client per stream
      if (!perStreamEventHubManagers.containsKey(streamId)) {
        LOG.info("Creating EventHub client manager for stream: " + streamId);
        EventHubClientManager perStreamEventHubClientManager =
            eventHubClientManagerFactory.getEventHubClientManager(systemName, streamId, config);
        perStreamEventHubClientManager.init();
        perStreamEventHubManagers.put(streamId, perStreamEventHubClientManager);
      }
      eventHubClientManager = perStreamEventHubManagers.get(streamId);
      perPartitionEventHubManagers.put(ssp, eventHubClientManager);
    }
    LOG.info("EventHub client created for ssp: " + ssp);
    Validate.notNull(eventHubClientManager,
        String.format("Fail to create or get EventHubClientManager for ssp=%s", ssp));
    return eventHubClientManager;
  }

  private synchronized void initializeEventHubsManagers() {
    LOG.info("Starting EventHubSystemConsumer. Count of SSPs registered: " + streamPartitionOffsets.entrySet().size());
    eventHubNonTransientError.set(null);
    // Create receivers for Event Hubs
    for (Map.Entry<SystemStreamPartition, String> entry : streamPartitionOffsets.entrySet()) {
      SystemStreamPartition ssp = entry.getKey();
      String streamId = config.getStreamId(ssp.getStream());
      Integer partitionId = ssp.getPartition().getPartitionId();
      String offset = entry.getValue();
      String consumerGroup = config.getStreamConsumerGroup(systemName, streamId);
      String namespace = config.getStreamNamespace(systemName, streamId);
      String entityPath = config.getStreamEntityPath(systemName, streamId);
      EventHubClientManager eventHubClientManager = createOrGetEventHubClientManagerForSSP(streamId, ssp);

      try {
        PartitionReceiver receiver;
        if (END_OF_STREAM.equals(offset)) {
          // If the offset is greater than the newest offset, use the use current Instant as
          // offset to fetch in Eventhub.
          receiver = eventHubClientManager.getEventHubClient()
              .createReceiverSync(consumerGroup, partitionId.toString(), EventPosition.fromEnqueuedTime(Instant.now()));
        } else {
          // EventHub will return the first message AFTER the offset that was specified in the fetch request.
          // If no such offset exists Eventhub will return an error.
          receiver = eventHubClientManager.getEventHubClient()
              .createReceiverSync(consumerGroup, partitionId.toString(),
                  EventPosition.fromOffset(offset, /* inclusiveFlag */false));
        }

        receiver.setPrefetchCount(prefetchCount);

        PartitionReceiveHandler handler =
            new PartitionReceiverHandlerImpl(ssp, eventReadRates.get(streamId), eventByteReadRates.get(streamId),
                consumptionLagMs.get(streamId), readErrors.get(streamId), interceptors.getOrDefault(streamId, null),
                config.getMaxEventCountPerPoll(systemName));

        // Timeout for EventHubClient receive
        receiver.setReceiveTimeout(DEFAULT_EVENTHUB_RECEIVER_TIMEOUT);

        // Start the receiver thread
        receiver.setReceiveHandler(handler);

        streamPartitionHandlers.put(ssp, handler);
        streamPartitionReceivers.put(ssp, receiver);
      } catch (Exception e) {
        throw new SamzaException(
            String.format("Failed to create receiver for EventHubs: namespace=%s, entity=%s, partitionId=%d", namespace,
                entityPath, partitionId), e);
      }
      LOG.info(String.format("Connection successfully started for namespace=%s, entity=%s ", namespace, entityPath));
    }
  }

  @Override
  public void start() {
    if (isStarted) {
      LOG.warn("Trying to start EventHubSystemConsumer while it's already started. Ignore the request.");
      return;
    }
    isStarted = true;
    initializeEventHubsManagers();
    LOG.info("EventHubSystemConsumer started");
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
    Throwable handlerError = eventHubNonTransientError.get();
    /*
     * We will retry for non transient error by instantiating a new EventHubs client if
     * 1. Last retry happened more than CONFIG_MIN_RETRY_INTERVAL_MS ms ago. Otherwise we ignore
     * 2. We haven't reached CONFIG_MAX_RETRY_COUNT allowed within the CONFIG_RETRY_WINDOW_MS window.
     *    Otherwise we throw
     */
    if (handlerError != null && clock.currentTimeMillis() - lastRetryTs > config.getMinRetryIntervalMs(systemName)) {
      int currentRetryCount = recentRetryAttempts.size();
      long maxRetryCount = config.getMaxRetryCount(systemName);
      if (currentRetryCount < maxRetryCount) {
        LOG.warn("Received non transient error. Will retry.", handlerError);
        LOG.info("Current retry count within window: {}. max retry count allowed: {}. window size: {} ms",
            currentRetryCount, maxRetryCount, config.getRetryWindowMs(systemName));
        long now = clock.currentTimeMillis();
        recentRetryAttempts.update(now);
        lastRetryTs = now;
        reconnectTaskStatus = reconnectTaskRunner.submit(this::renewEventHubsClient);
      } else {
        LOG.error("Retries exhausted. Reached max allowed retries: ({}) within window {} ms", currentRetryCount,
            config.getRetryWindowMs(systemName));
        String msg = "Received a non transient error from event hub partition receiver";
        throw new SamzaException(msg, handlerError);
      }
    }

    return super.poll(systemStreamPartitions, timeout);
  }

  private synchronized void renewEventHubsClient() {
    try {
      LOG.info("Start to renew eventhubs client");
      shutdownEventHubsManagers(); // The shutdown is in parallel and time bounded
      initializeEventHubsManagers();
    } catch (Exception e) {
      LOG.error("Failed to renew eventhubs client", e);
      eventHubNonTransientError.set(e);
    }
  }

  private void renewPartitionReceiver(SystemStreamPartition ssp) {
    String streamId = config.getStreamId(ssp.getStream());
    EventHubClientManager eventHubClientManager = perPartitionEventHubManagers.get(ssp);
    String offset = streamPartitionOffsets.get(ssp);
    Integer partitionId = ssp.getPartition().getPartitionId();
    String consumerGroup = config.getStreamConsumerGroup(ssp.getSystem(), streamId);

    try {
      // Close current receiver
      streamPartitionReceivers.get(ssp).close().get(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

      // Recreate receiver
      PartitionReceiver receiver = eventHubClientManager.getEventHubClient()
          .createReceiverSync(consumerGroup, partitionId.toString(),
              EventPosition.fromOffset(offset, !offset.equals(EventHubSystemConsumer.START_OF_STREAM)));

      receiver.setPrefetchCount(prefetchCount);

      // Timeout for EventHubClient receive
      receiver.setReceiveTimeout(DEFAULT_EVENTHUB_RECEIVER_TIMEOUT);

      // Create and start receiver thread with handler
      receiver.setReceiveHandler(streamPartitionHandlers.get(ssp));
      streamPartitionReceivers.put(ssp, receiver);
    } catch (Exception e) {
      eventHubNonTransientError.set(new SamzaException(
          String.format("Failed to recreate receiver for EventHubs after ReceiverHandlerError (ssp=%s)", ssp), e));
    }
  }

  private synchronized void shutdownEventHubsManagers() {
    // There could be potentially many Receivers and EventHubManagers, so close the managers in parallel
    LOG.info("Start shutting down eventhubs receivers");
    ShutdownUtil.boundedShutdown(streamPartitionReceivers.values().stream().map(receiver -> new Runnable() {
      @Override
      public void run() {
        try {
          receiver.close().get(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          LOG.error("Failed to shutdown receiver.", e);
        }
      }
    }).collect(Collectors.toList()), "EventHubSystemConsumer.Receiver#close", DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);

    LOG.info("Start shutting down eventhubs managers");
    ShutdownUtil.boundedShutdown(perPartitionEventHubManagers.values().stream().map(manager -> new Runnable() {
      @Override
      public void run() {
        try {
          manager.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
        } catch (Exception e) {
          LOG.error("Failed to shutdown eventhubs manager.", e);
        }
      }
    }).collect(Collectors.toList()), "EventHubSystemConsumer.ClientManager#close", DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);

    perPartitionEventHubManagers.clear();
    perStreamEventHubManagers.clear();
  }

  @Override
  public void stop() {
    LOG.info("Stopping event hub system consumer...");
    try {
      reconnectTaskRunner.shutdown();
      shutdownEventHubsManagers();
      isStarted = false;
    } catch (Exception e) {
      LOG.warn("Exception during stop.", e);
    }
    LOG.info("Event hub system consumer stopped.");
  }

  @Override
  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
    return new LinkedBlockingQueue<>(config.getConsumerBufferCapacity(systemName));
  }

  protected class PartitionReceiverHandlerImpl implements PartitionReceiveHandler {

    private final Counter eventReadRate;
    private final Counter eventByteReadRate;
    private final SamzaHistogram readLatency;
    private final Counter errorRate;
    private final Interceptor interceptor;
    private final Integer maxEventCount;
    SystemStreamPartition ssp;

    PartitionReceiverHandlerImpl(SystemStreamPartition ssp, Counter eventReadRate, Counter eventByteReadRate,
        SamzaHistogram readLatency, Counter readErrors, Interceptor interceptor, int maxEventCount) {
      this.ssp = ssp;
      this.eventReadRate = eventReadRate;
      this.eventByteReadRate = eventByteReadRate;
      this.readLatency = readLatency;
      this.errorRate = readErrors;
      this.interceptor = interceptor;
      this.maxEventCount = maxEventCount;
    }

    @Override
    public int getMaxEventCount() {
      return this.maxEventCount;
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

      long latencyMs = Duration.between(event.getSystemProperties().getEnqueuedTime(), Instant.now()).toMillis();
      readLatency.update(latencyMs);
      aggConsumptionLagMs.update(latencyMs);
    }

    @Override
    public void onError(Throwable throwable) {
      errorRate.inc();
      aggReadErrors.inc();

      if (throwable instanceof EventHubException) {
        EventHubException busException = (EventHubException) throwable;

        if (busException.getIsTransient()) {
          LOG.warn(
              String.format("Received transient exception from EH client. Renew partition receiver for ssp: %s", ssp),
              throwable);
          try {
            // Add a fixed delay so that we don't keep retrying when there are long-lasting failures
            Thread.sleep(Duration.ofSeconds(2).toMillis());
          } catch (InterruptedException e) {
            LOG.warn("Interrupted during sleep before renew", e);
          }
          // Retry creating a receiver since error likely due to timeout
          renewPartitionReceiver(ssp);
          return;
        }
      }

      LOG.error(String.format("Received non transient exception from EH client for ssp: %s", ssp), throwable);
      // Propagate non transient or unknown errors
      eventHubNonTransientError.set(throwable);
    }
  }
}
