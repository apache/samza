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

package org.apache.samza;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
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
 */
public abstract class AsyncSystemProducer implements SystemProducer {

  private static final Logger LOG = LoggerFactory.getLogger(NoFlushAsyncSystemProducer.class.getName());

  /**
   * The constant CONFIG_STREAM_LIST.
   */
  public static final String CONFIG_STREAM_LIST = "systems.%s.stream.list";
  private static final String SEND_ERRORS = "sendErrors";
  private static final String SEND_CALLBACK_LATENCY = "sendCallbackLatency";
  /**
   * The constant AGGREGATE.
   */
  protected static final String AGGREGATE = "aggregate";

  /**
   * The Stream ids.
   */
  protected final List<String> streamIds;
  /**
   * The Physical to stream ids.
   */
  protected final Map<String, String> physicalToStreamIds;
  /**
   * The Registry.
   */
  protected final MetricsRegistry registry;

  /**
   * The Pending futures.
   */
  protected final Set<CompletableFuture<Void>> pendingFutures = ConcurrentHashMap.newKeySet();

  private final AtomicReference<Throwable> sendExceptionOnCallback = new AtomicReference<>(null);

  private static Counter aggSendErrors = null;
  private static SamzaHistogram aggSendCallbackLatency = null;
  private final HashMap<String, SamzaHistogram> sendCallbackLatency = new HashMap<>();
  private final HashMap<String, Counter> sendErrors = new HashMap<>();

  /**
   * Instantiates a new Async system producer.
   *
   * @param systemName the system name
   * @param config the config
   * @param registry the registry
   */
  public AsyncSystemProducer(String systemName, Config config, MetricsRegistry registry) {
    StreamConfig sconfig = new StreamConfig(config);
    streamIds = config.getList(String.format(CONFIG_STREAM_LIST, systemName));
    physicalToStreamIds =
        streamIds.stream().collect(Collectors.toMap(sconfig::getPhysicalName, Function.identity()));
    this.registry = registry;
  }

  /**
   * {@inheritDoc}
   */
  public void send(String source, OutgoingMessageEnvelope envelope) {
    checkCallbackThrowable("Received exception on message send");
    CompletableFuture<Void> sendResult = sendAsync(source, envelope);
    pendingFutures.add(sendResult);

    String streamName = envelope.getSystemStream().getStream();
    String streamId = physicalToStreamIds.getOrDefault(streamName, streamName);
    long afterSendTimeMs = System.currentTimeMillis();

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
        sendCallbackLatency.put(streamId, new SamzaHistogram(registry, streamId, SEND_CALLBACK_LATENCY));
        sendErrors.put(streamId, registry.newCounter(streamId, SEND_ERRORS));
      });

    aggSendCallbackLatency = new SamzaHistogram(registry, AGGREGATE, SEND_CALLBACK_LATENCY);
    aggSendErrors = registry.newCounter(AGGREGATE, SEND_ERRORS);
  }

  /**
   * Sends a specified message envelope from a specified Samza source.
   * @param source String representing the source of the message.
   * @param envelope Aggregate object representing the serialized message to send from the source.
   * @return the completable future for the async send call
   */
  public abstract CompletableFuture<Void> sendAsync(String source, OutgoingMessageEnvelope envelope);

  /**
   * Check callback throwable.
   *
   * @param msg the msg
   */
  protected void checkCallbackThrowable(String msg) {
    // Check for send errors from EventHub side
    Throwable sendThrowable = sendExceptionOnCallback.get();
    if (sendThrowable != null) {
      LOG.error(msg, sendThrowable);
      throw new SamzaException(msg, sendThrowable);
    }
  }

  /**
   * Gets pending futures.
   *
   * @return the pending futures
   */
  public Collection<CompletableFuture<Void>> getPendingFutures() {
    return pendingFutures;
  }
}
