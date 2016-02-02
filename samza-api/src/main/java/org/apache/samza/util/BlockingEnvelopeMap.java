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

package org.apache.samza.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * BlockingEnvelopeMap is a helper class for SystemConsumer implementations.
 * Samza's poll() requirements make implementing SystemConsumers somewhat
 * tricky. BlockingEnvelopeMap is provided to help other developers write
 * SystemConsumers. The intended audience is not those writing Samza jobs, but
 * rather those extending Samza to consume from new types of stream providers
 * and other systems.
 * </p>
 * 
 * <p>
 * SystemConsumers that implement BlockingEnvelopeMap need to add messages using
 * {@link #put(org.apache.samza.system.SystemStreamPartition, org.apache.samza.system.IncomingMessageEnvelope) put}
 * (or {@link #putAll(org.apache.samza.system.SystemStreamPartition, java.util.List) putAll}),
 * and update noMoreMessage using setIsAtHead. The noMoreMessage variable is used
 * to determine whether a SystemStreamPartition is "caught up" (has read all
 * possible messages from the underlying system). For example, with a Kafka
 * system, noMoreMessages would be set to true when the last message offset
 * returned is equal to the offset high watermark for a given topic/partition.
 * </p>
 * The BlockingEnvelopeMap is backed by a concurrent map, which allows concurrent
 * put or putAll calls to be thread safe without external synchronization.
 */
public abstract class BlockingEnvelopeMap implements SystemConsumer {
  private final BlockingEnvelopeMapMetrics metrics;
  private final ConcurrentHashMap<SystemStreamPartition, BlockingQueue<IncomingMessageEnvelope>> bufferedMessages;
  private final ConcurrentHashMap<SystemStreamPartition, AtomicLong> bufferedMessagesSize;  // size in bytes per SystemStreamPartition
  private final Map<SystemStreamPartition, Boolean> noMoreMessage;
  private final Clock clock;
  protected final boolean fetchLimitByBytesEnabled;

  public BlockingEnvelopeMap() {
    this(new NoOpMetricsRegistry());
  }

  public BlockingEnvelopeMap(MetricsRegistry metricsRegistry) {
    this(metricsRegistry, new Clock() {
      public long currentTimeMillis() {
        return System.currentTimeMillis();
      }
    });
  }

  public BlockingEnvelopeMap(MetricsRegistry metricsRegistry, Clock clock) {
    this(metricsRegistry, clock, null, false);
  }

  public BlockingEnvelopeMap(MetricsRegistry metricsRegistry, Clock clock, String metricsGroupName, boolean fetchLimitByBytesEnabled) {
    metricsGroupName = (metricsGroupName == null) ? this.getClass().getName() : metricsGroupName;
    this.metrics = new BlockingEnvelopeMapMetrics(metricsGroupName, metricsRegistry);
    this.bufferedMessages = new ConcurrentHashMap<SystemStreamPartition, BlockingQueue<IncomingMessageEnvelope>>();
    this.noMoreMessage = new ConcurrentHashMap<SystemStreamPartition, Boolean>();
    this.clock = clock;
    this.fetchLimitByBytesEnabled = fetchLimitByBytesEnabled;
    // Created when size is disabled for code simplification, and as the overhead is negligible.
    this.bufferedMessagesSize = new ConcurrentHashMap<SystemStreamPartition, AtomicLong>();
  }

  /**
   * {@inheritDoc}
   */
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    metrics.initMetrics(systemStreamPartition);
    bufferedMessages.putIfAbsent(systemStreamPartition, newBlockingQueue());
    // Created when size is disabled for code simplification, and the overhead is negligible.
    bufferedMessagesSize.putIfAbsent(systemStreamPartition, new AtomicLong(0));
  }

  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
    return new LinkedBlockingQueue<IncomingMessageEnvelope>();
  }

  /**
   * {@inheritDoc}
   */
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
    long stopTime = clock.currentTimeMillis() + timeout;
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> messagesToReturn = new HashMap<SystemStreamPartition, List<IncomingMessageEnvelope>>();

    metrics.incPoll();

    for (SystemStreamPartition systemStreamPartition : systemStreamPartitions) {
      BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);
      List<IncomingMessageEnvelope> outgoingList = new ArrayList<IncomingMessageEnvelope>(queue.size());

      if (queue.size() > 0) {
        queue.drainTo(outgoingList);
      } else if (timeout != 0) {
        IncomingMessageEnvelope envelope = null;

        // How long we can legally block (if timeout > 0)
        long timeRemaining = stopTime - clock.currentTimeMillis();

        if (timeout == SystemConsumer.BLOCK_ON_OUTSTANDING_MESSAGES) {
          // Block until we get at least one message, or until we catch up to
          // the head of the stream.
          while (envelope == null && !isAtHead(systemStreamPartition)) {
            metrics.incBlockingPoll(systemStreamPartition);
            envelope = queue.poll(1000, TimeUnit.MILLISECONDS);
          }
        } else if (timeout > 0 && timeRemaining > 0) {
          // Block until we get at least one message.
          metrics.incBlockingTimeoutPoll(systemStreamPartition);
          envelope = queue.poll(timeRemaining, TimeUnit.MILLISECONDS);
        }

        // If we got a message, add it.
        if (envelope != null) {
          outgoingList.add(envelope);
          // Drain any remaining messages without blocking.
          queue.drainTo(outgoingList);
        }
      }

      if (outgoingList.size() > 0) {
        messagesToReturn.put(systemStreamPartition, outgoingList);
        if (fetchLimitByBytesEnabled) {
          subtractSizeOnQDrain(systemStreamPartition, outgoingList);
        }
      }
    }

    return messagesToReturn;
  }

  private void subtractSizeOnQDrain(SystemStreamPartition systemStreamPartition, List<IncomingMessageEnvelope> outgoingList) {
    long outgoingListBytes = 0;
    for (IncomingMessageEnvelope envelope : outgoingList) {
      outgoingListBytes += envelope.getSize();
    }
    // subtract the size of the messages dequeued.
    bufferedMessagesSize.get(systemStreamPartition).addAndGet(-1 * outgoingListBytes);
  }

  /**
   * Place a new {@link org.apache.samza.system.IncomingMessageEnvelope} on the
   * queue for the specified {@link org.apache.samza.system.SystemStreamPartition}.
   *
   * @param systemStreamPartition SystemStreamPartition that owns the envelope
   * @param envelope Message for specified SystemStreamPartition
   * @throws InterruptedException from underlying concurrent collection
   */
  protected void put(SystemStreamPartition systemStreamPartition, IncomingMessageEnvelope envelope) throws InterruptedException {
    bufferedMessages.get(systemStreamPartition).put(envelope);
    if (fetchLimitByBytesEnabled) {
      bufferedMessagesSize.get(systemStreamPartition).addAndGet(envelope.getSize());
    }
  }

  /**
   * Place a collection of {@link org.apache.samza.system.IncomingMessageEnvelope}
   * on the queue for the specified {@link org.apache.samza.system.SystemStreamPartition}.
   * <p>
   * Insertion of all the messages into the queue is not guaranteed to be done
   * atomically.
   * </p>
   *
   * @param systemStreamPartition SystemStreamPartition that owns the envelope
   * @param envelopes Messages for specified SystemStreamPartition
   * @throws InterruptedException from underlying concurrent collection
   */
  protected void putAll(SystemStreamPartition systemStreamPartition, List<IncomingMessageEnvelope> envelopes) throws InterruptedException {
    BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);

    for (IncomingMessageEnvelope envelope : envelopes) {
      queue.put(envelope);
    }
  }

  public int getNumMessagesInQueue(SystemStreamPartition systemStreamPartition) {
    BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);

    if (queue == null) {
      throw new NullPointerException("Attempting to get queue for " + systemStreamPartition + ", but the system/stream/partition was never registered.");
    } else {
      return queue.size();
    }
  }

  public long getMessagesSizeInQueue(SystemStreamPartition systemStreamPartition) {
    AtomicLong sizeInBytes = bufferedMessagesSize.get(systemStreamPartition);

    if (sizeInBytes == null) {
      throw new NullPointerException("Attempting to get size for " + systemStreamPartition + ", but the system/stream/partition was never registered. or fetch");
    } else {
      return sizeInBytes.get();
    }
  }

  protected Boolean setIsAtHead(SystemStreamPartition systemStreamPartition, boolean isAtHead) {
    metrics.setNoMoreMessages(systemStreamPartition, isAtHead);
    return noMoreMessage.put(systemStreamPartition, isAtHead);
  }

  protected boolean isAtHead(SystemStreamPartition systemStreamPartition) {
    Boolean isAtHead = noMoreMessage.get(systemStreamPartition);

    return getNumMessagesInQueue(systemStreamPartition) == 0 && isAtHead != null && isAtHead.equals(true);
  }

  public class BlockingEnvelopeMapMetrics {
    private final String group;
    private final MetricsRegistry metricsRegistry;
    private final ConcurrentHashMap<SystemStreamPartition, Gauge<Boolean>> noMoreMessageGaugeMap;
    private final ConcurrentHashMap<SystemStreamPartition, Counter> blockingPollCountMap;
    private final ConcurrentHashMap<SystemStreamPartition, Counter> blockingPollTimeoutCountMap;
    private final Counter pollCount;

    public BlockingEnvelopeMapMetrics(String group, MetricsRegistry metricsRegistry) {
      this.group = group;
      this.metricsRegistry = metricsRegistry;
      this.noMoreMessageGaugeMap = new ConcurrentHashMap<SystemStreamPartition, Gauge<Boolean>>();
      this.blockingPollCountMap = new ConcurrentHashMap<SystemStreamPartition, Counter>();
      this.blockingPollTimeoutCountMap = new ConcurrentHashMap<SystemStreamPartition, Counter>();
      this.pollCount = metricsRegistry.newCounter(group, "poll-count");
    }

    public void initMetrics(SystemStreamPartition systemStreamPartition) {
      this.noMoreMessageGaugeMap.putIfAbsent(systemStreamPartition, metricsRegistry.<Boolean>newGauge(group, "no-more-messages-" + systemStreamPartition, false));
      this.blockingPollCountMap.putIfAbsent(systemStreamPartition, metricsRegistry.newCounter(group, "blocking-poll-count-" + systemStreamPartition));
      this.blockingPollTimeoutCountMap.putIfAbsent(systemStreamPartition, metricsRegistry.newCounter(group, "blocking-poll-timeout-count-" + systemStreamPartition));

      metricsRegistry.<Integer>newGauge(group, new BufferGauge(systemStreamPartition, "buffered-message-count-" + systemStreamPartition));
      if (fetchLimitByBytesEnabled) {
        metricsRegistry.<Long>newGauge(group, new BufferSizeGauge(systemStreamPartition, "buffered-message-size-" + systemStreamPartition));
      }
    }

    public void setNoMoreMessages(SystemStreamPartition systemStreamPartition, boolean noMoreMessages) {
      this.noMoreMessageGaugeMap.get(systemStreamPartition).set(noMoreMessages);
    }

    public void incBlockingPoll(SystemStreamPartition systemStreamPartition) {
      this.blockingPollCountMap.get(systemStreamPartition).inc();
    }

    public void incBlockingTimeoutPoll(SystemStreamPartition systemStreamPartition) {
      this.blockingPollTimeoutCountMap.get(systemStreamPartition).inc();
    }

    public void incPoll() {
      this.pollCount.inc();
    }
  }

  public class BufferGauge extends Gauge<Integer> {
    private final SystemStreamPartition systemStreamPartition;

    public BufferGauge(SystemStreamPartition systemStreamPartition, String name) {
      super(name, 0);

      this.systemStreamPartition = systemStreamPartition;
    }

    @Override
    public Integer getValue() {
      Queue<IncomingMessageEnvelope> envelopes = bufferedMessages.get(systemStreamPartition);

      if (envelopes == null) {
        return 0;
      }

      return envelopes.size();
    }
  }

  public class BufferSizeGauge extends Gauge<Long> {
    private final SystemStreamPartition systemStreamPartition;

    public BufferSizeGauge(SystemStreamPartition systemStreamPartition, String name) {
      super(name, 0L);

      this.systemStreamPartition = systemStreamPartition;
    }

    @Override
    public Long getValue() {
      AtomicLong sizeInBytes = bufferedMessagesSize.get(systemStreamPartition);

      if (sizeInBytes == null) {
        return 0L;
      }

      return sizeInBytes.get();
    }
  }
}
