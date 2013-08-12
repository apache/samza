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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

/**
 * <p>
 * BlockingEnvelopeMap is a helper class for SystemConsumer implementations.
 * Samza's poll() requirements make implementing SystemConsumers somewhat
 * tricky. BlockingEnvelopeMap is provided to help other developers write
 * SystemConsumers.
 * </p>
 * 
 * <p>
 * SystemConsumers that implement BlockingEnvelopeMap need to add messages using
 * add (or addAll), and update noMoreMessage using setIsAtHead. The
 * noMoreMessage variable is used to determine whether a SystemStreamPartition
 * is "caught up" (has read all possible messages from the underlying system).
 * For example, with a Kafka system, noMoreMessages would be set to true when
 * the last message offset returned is equal to the offset high watermark for a
 * given topic/partition.
 * </p>
 */
public abstract class BlockingEnvelopeMap implements SystemConsumer {
  private final BlockingEnvelopeMapMetrics metrics;
  private final ConcurrentHashMap<SystemStreamPartition, BlockingQueue<IncomingMessageEnvelope>> bufferedMessages;
  private final Map<SystemStreamPartition, Boolean> noMoreMessage;
  private final int queueSize;
  private final Clock clock;

  public BlockingEnvelopeMap() {
    this(1000, new NoOpMetricsRegistry());
  }

  public BlockingEnvelopeMap(Clock clock) {
    this(1000, new NoOpMetricsRegistry(), clock);
  }

  public BlockingEnvelopeMap(int queueSize, MetricsRegistry metricsRegistry) {
    this(queueSize, metricsRegistry, new Clock() {
      public long currentTimeMillis() {
        return System.currentTimeMillis();
      }
    });
  }

  public BlockingEnvelopeMap(int queueSize, MetricsRegistry metricsRegistry, Clock clock) {
    this.metrics = new BlockingEnvelopeMapMetrics(queueSize, metricsRegistry);
    this.bufferedMessages = new ConcurrentHashMap<SystemStreamPartition, BlockingQueue<IncomingMessageEnvelope>>();
    this.noMoreMessage = new ConcurrentHashMap<SystemStreamPartition, Boolean>();
    this.queueSize = queueSize;
    this.clock = clock;
  }

  public void register(SystemStreamPartition systemStreamPartition, String lastReadOffset) {
    metrics.initMetrics(systemStreamPartition);
    bufferedMessages.putIfAbsent(systemStreamPartition, newBlockingQueue());
  }

  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
    return new ArrayBlockingQueue<IncomingMessageEnvelope>(queueSize);
  }

  public List<IncomingMessageEnvelope> poll(Map<SystemStreamPartition, Integer> systemStreamPartitionAndMaxPerStream, long timeout) throws InterruptedException {
    long stopTime = clock.currentTimeMillis() + timeout;
    List<IncomingMessageEnvelope> messagesToReturn = new ArrayList<IncomingMessageEnvelope>();

    metrics.incPoll();

    for (Map.Entry<SystemStreamPartition, Integer> systemStreamPartitionAndMaxCount : systemStreamPartitionAndMaxPerStream.entrySet()) {
      SystemStreamPartition systemStreamPartition = systemStreamPartitionAndMaxCount.getKey();
      Integer numMessages = systemStreamPartitionAndMaxCount.getValue();
      BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);
      IncomingMessageEnvelope envelope = null;
      List<IncomingMessageEnvelope> systemStreamPartitionMessages = new ArrayList<IncomingMessageEnvelope>();

      metrics.incPoll(systemStreamPartition);

      // First, drain all messages up to numMessages without blocking.
      // Stop when we've filled the request (max numMessages), or when
      // we get a null envelope back.
      for (int i = 0; i < numMessages && (i == 0 || envelope != null); ++i) {
        envelope = queue.poll();

        if (envelope != null) {
          systemStreamPartitionMessages.add(envelope);
        }
      }

      metrics.decBufferedMessageCount(systemStreamPartition, systemStreamPartitionMessages.size());

      // Now block if blocking is allowed and we have no messages.
      if (systemStreamPartitionMessages.size() == 0) {
        // How long we can legally block (if timeout > 0)
        long timeRemaining = stopTime - clock.currentTimeMillis();

        if (timeout == SystemConsumer.BLOCK_ON_OUTSTANDING_MESSAGES) {
          while (systemStreamPartitionMessages.size() < numMessages && !isAtHead(systemStreamPartition)) {
            metrics.incBlockingPoll(systemStreamPartition);
            envelope = queue.poll(1000, TimeUnit.MILLISECONDS);

            if (envelope != null) {
              systemStreamPartitionMessages.add(envelope);
              metrics.decBufferedMessageCount(systemStreamPartition, 1);
            }
          }
        } else if (timeout > 0 && timeRemaining > 0) {
          metrics.incBlockingTimeoutPoll(systemStreamPartition);
          envelope = queue.poll(timeRemaining, TimeUnit.MILLISECONDS);

          if (envelope != null) {
            systemStreamPartitionMessages.add(envelope);
            metrics.decBufferedMessageCount(systemStreamPartition, 1);
          }
        }
      }

      messagesToReturn.addAll(systemStreamPartitionMessages);
    }

    return messagesToReturn;
  }

  protected void add(SystemStreamPartition systemStreamPartition, IncomingMessageEnvelope envelope) throws InterruptedException {
    bufferedMessages.get(systemStreamPartition).put(envelope);
    metrics.incBufferedMessageCount(systemStreamPartition, 1);
  }

  protected void addAll(SystemStreamPartition systemStreamPartition, List<IncomingMessageEnvelope> envelopes) throws InterruptedException {
    BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);

    for (IncomingMessageEnvelope envelope : envelopes) {
      queue.put(envelope);
    }

    metrics.incBufferedMessageCount(systemStreamPartition, envelopes.size());
  }

  protected int getNumMessagesInQueue(SystemStreamPartition systemStreamPartition) {
    BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);

    if (queue == null) {
      return 0;
    } else {
      return queue.size();
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

  public static final class BlockingEnvelopeMapMetrics {
    private static final String GROUP = "samza.consumers";

    private final MetricsRegistry metricsRegistry;
    private final ConcurrentHashMap<SystemStreamPartition, Counter> bufferedMessageCountMap;
    private final ConcurrentHashMap<SystemStreamPartition, Gauge<Boolean>> noMoreMessageGaugeMap;
    private final ConcurrentHashMap<SystemStreamPartition, Counter> pollCountMap;
    private final ConcurrentHashMap<SystemStreamPartition, Counter> blockingPollCountMap;
    private final ConcurrentHashMap<SystemStreamPartition, Counter> blockingPollTimeoutCountMap;
    // TODO use the queueSize gauge
    private final Gauge<Integer> queueSize;
    private final Counter pollCount;

    public BlockingEnvelopeMapMetrics(int queueSize, MetricsRegistry metricsRegistry) {
      this.metricsRegistry = metricsRegistry;
      this.bufferedMessageCountMap = new ConcurrentHashMap<SystemStreamPartition, Counter>();
      this.noMoreMessageGaugeMap = new ConcurrentHashMap<SystemStreamPartition, Gauge<Boolean>>();
      this.pollCountMap = new ConcurrentHashMap<SystemStreamPartition, Counter>();
      this.blockingPollCountMap = new ConcurrentHashMap<SystemStreamPartition, Counter>();
      this.blockingPollTimeoutCountMap = new ConcurrentHashMap<SystemStreamPartition, Counter>();
      this.queueSize = metricsRegistry.<Integer> newGauge(GROUP, "QueueSize", queueSize);
      this.pollCount = metricsRegistry.newCounter(GROUP, "PollCount");
    }

    public void initMetrics(SystemStreamPartition systemStreamPartition) {
      this.bufferedMessageCountMap.putIfAbsent(systemStreamPartition, metricsRegistry.newCounter(GROUP, "BufferedMessageCount-" + systemStreamPartition));
      this.noMoreMessageGaugeMap.putIfAbsent(systemStreamPartition, metricsRegistry.<Boolean> newGauge(GROUP, "NoMoreMessages-" + systemStreamPartition, false));
      this.pollCountMap.putIfAbsent(systemStreamPartition, metricsRegistry.newCounter(GROUP, "PollCount-" + systemStreamPartition));
      this.blockingPollCountMap.putIfAbsent(systemStreamPartition, metricsRegistry.newCounter(GROUP, "BlockingPollCount-" + systemStreamPartition));
      this.blockingPollTimeoutCountMap.putIfAbsent(systemStreamPartition, metricsRegistry.newCounter(GROUP, "BlockingPollTimeoutCount-" + systemStreamPartition));
    }

    public void incBufferedMessageCount(SystemStreamPartition systemStreamPartition, int count) {
      this.bufferedMessageCountMap.get(systemStreamPartition).inc(count);
    }

    public void decBufferedMessageCount(SystemStreamPartition systemStreamPartition, int count) {
      this.bufferedMessageCountMap.get(systemStreamPartition).dec(count);
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

    public void incPoll(SystemStreamPartition systemStreamPartition) {
      this.pollCountMap.get(systemStreamPartition).inc();
    }

    public void incPoll() {
      this.pollCount.inc();
    }
  }
}
