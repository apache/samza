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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.eventhub.metrics.SamzaHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SystemProducer Helper that can be used to implement SystemProducer that provide async send APIs
 * e.g. Kinesis, Event Hubs, etc..
 * To implement an AsyncSystemProducer, you need to implement {@link AsyncSystemProducer#sendAsync}
 */
public abstract class AsyncSystemProducer implements SystemProducer {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncSystemProducer.class.getName());

  private static final long DEFAULT_FLUSH_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  /**
   * The constant CONFIG_STREAM_LIST. This config is used to get the list of streams produced by this EventHub system.
   */
  public static final String CONFIG_STREAM_LIST = "systems.%s.stream.list";

  private static final String SEND_ERRORS = "sendErrors";
  private static final String SEND_CALLBACK_LATENCY = "sendCallbackLatency";
  private static final String SEND_LATENCY = "sendLatency";

  /**
   * The constant AGGREGATE.
   */
  protected static final String AGGREGATE = "aggregate";

  /**
   * The Stream ids.
   */
  protected final List<String> streamIds;
  /**
   * The Map between Physical streamName to virtual Samza streamids.
   */
  protected final Map<String, String> physicalToStreamIds;

  /**
   * The Samza metrics registry used to store all the metrics emitted.
   */
  protected final MetricsRegistry metricsRegistry;

  /**
   * The Pending futures corresponding to the send calls that are still in flight and for which we haven't
   * received the call backs yet.
   * This needs to be concurrent because it is accessed across the send/flush thread and callback thread.
   */
  protected final Set<CompletableFuture<Void>> pendingFutures = ConcurrentHashMap.newKeySet();

  private final AtomicReference<Throwable> sendExceptionOnCallback = new AtomicReference<>(null);

  private static Counter aggSendErrors = null;
  private static SamzaHistogram aggSendLatency = null;
  private static SamzaHistogram aggSendCallbackLatency = null;

  private final HashMap<String, SamzaHistogram> sendLatency = new HashMap<>();
  private final HashMap<String, SamzaHistogram> sendCallbackLatency = new HashMap<>();
  private final HashMap<String, Counter> sendErrors = new HashMap<>();
  private final ScheduledExecutorService executorService;

  /**
   * Instantiates a new Async system producer.
   *
   * @param systemName the system name
   * @param config the config
   * @param metricsRegistry the registry
   */
  public AsyncSystemProducer(String systemName, Config config, MetricsRegistry metricsRegistry) {
    StreamConfig sconfig = new StreamConfig(config);
    streamIds = config.getList(String.format(CONFIG_STREAM_LIST, systemName));
    physicalToStreamIds =
        streamIds.stream().collect(Collectors.toMap(sconfig::getPhysicalName, Function.identity()));
    this.metricsRegistry = metricsRegistry;
    ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder().setNameFormat("Samza Histogram Poll Thread-%d");
    executorService = Executors.newScheduledThreadPool(1, threadFactoryBuilder.build());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void send(String source, OutgoingMessageEnvelope envelope) {
    checkForSendCallbackErrors("Received exception on message send");

    String streamName = envelope.getSystemStream().getStream();
    String streamId = physicalToStreamIds.getOrDefault(streamName, streamName);

    long beforeSendTimeMs = System.currentTimeMillis();

    CompletableFuture<Void> sendResult = sendAsync(source, envelope);

    long afterSendTimeMs = System.currentTimeMillis();

    long latencyMs = afterSendTimeMs - beforeSendTimeMs;
    sendLatency.get(streamId).update(latencyMs);
    aggSendLatency.update(latencyMs);

    pendingFutures.add(sendResult);

    // Auto update the metrics and possible throwable when futures are complete.
    sendResult.handle((aVoid, throwable) -> {
        long callbackLatencyMs = System.currentTimeMillis() - afterSendTimeMs;
        sendCallbackLatency.get(streamId).update(callbackLatencyMs);
        aggSendCallbackLatency.update(callbackLatencyMs);
        if (throwable != null) {
          sendErrors.get(streamId).inc();
          aggSendErrors.inc();
          LOG.error("Send message to event hub: {} failed with exception: ", streamId, throwable);
          sendExceptionOnCallback.compareAndSet(null, throwable);
        }
        return aVoid;
      });
  }

  public void start() {
    streamIds.forEach(streamId -> {
        sendCallbackLatency.put(streamId, new SamzaHistogram(metricsRegistry, streamId, SEND_CALLBACK_LATENCY, executorService));
        sendLatency.put(streamId, new SamzaHistogram(metricsRegistry, streamId, SEND_LATENCY, executorService));
        sendErrors.put(streamId, metricsRegistry.newCounter(streamId, SEND_ERRORS));
      });

    if (aggSendLatency == null) {
      aggSendLatency = new SamzaHistogram(metricsRegistry, AGGREGATE, SEND_LATENCY, executorService);
      aggSendCallbackLatency = new SamzaHistogram(metricsRegistry, AGGREGATE, SEND_CALLBACK_LATENCY, executorService);
      aggSendErrors = metricsRegistry.newCounter(AGGREGATE, SEND_ERRORS);
    }
  }

  /**
   * Default implementation of the flush that just waits for all the pendingFutures to be complete.
   * SystemProducer should override this, if the underlying system provides flush semantics.
   * @param source String representing the source of the message.
   */
  @Override
  public synchronized void flush(String source) {
    long incompleteSends = pendingFutures.stream().filter(x -> !x.isDone()).count();
    LOG.info("Trying to flush pending {} sends.", incompleteSends);
    checkForSendCallbackErrors("Received exception on message send.");
    CompletableFuture<Void> future =
        CompletableFuture.allOf(pendingFutures.toArray(new CompletableFuture[pendingFutures.size()]));

    try {
      // Block until all the pending sends are complete or timeout.
      future.get(DEFAULT_FLUSH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      incompleteSends = pendingFutures.stream().filter(x -> !x.isDone()).count();
      String msg = String.format("Flush failed with error. Total pending sends %d", incompleteSends);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }

    pendingFutures.clear();

    checkForSendCallbackErrors("Sending one or more of the messages failed during flush.");
  }

  /**
   * Sends a specified message envelope from a specified Samza source.
   * @param source String representing the source of the message.
   * @param envelope Aggregate object representing the serialized message to send from the source.
   * @return the completable future for the async send call
   */
  public abstract CompletableFuture<Void> sendAsync(String source, OutgoingMessageEnvelope envelope);

  /**
   * This method is used to check whether there were any previous exceptions in the send call backs.
   * And if any exception occurs the producer is considered to be stuck/broken
   * @param msg the msg that is passed to the exception.
   */
  protected void checkForSendCallbackErrors(String msg) {
    // Check for send errors
    Throwable sendThrowable = sendExceptionOnCallback.get();
    if (sendThrowable != null) {
      LOG.error(msg, sendThrowable);
      throw new SamzaException(msg, sendThrowable);
    }
  }
}
