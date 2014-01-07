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

package org.apache.samza.system.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.Clock;

/**
 * MockSystemConsumer is a class that simulates a multi-threaded consumer that
 * uses BlockingEnvelopeMap. The primary use for this class is to do performance
 * testing.
 * 
 * This class works by starting up (threadCount) threads. Each thread adds
 * (messagesPerBatch) to the BlockingEnvelopeMap, then sleeps for
 * (brokerSleepMs). The sleep is important to simulate network latency when
 * executing a fetch against a remote streaming system (i.e. Kafka).
 */
public class MockSystemConsumer extends BlockingEnvelopeMap {
  private final int messagesPerBatch;
  private final int threadCount;
  private final int brokerSleepMs;

  /**
   * The SystemStreamPartitions that this consumer is in charge of.
   */
  private final Set<SystemStreamPartition> ssps;

  /**
   * The consumer threads that are putting IncomingMessageEnvelopes into
   * BlockingEnvelopeMap.
   */
  private List<Thread> threads;

  /**
   * 
   * @param messagesPerBatch
   *          The number of messages to add to the BlockingEnvelopeMap before
   *          sleeping.
   * @param threadCount
   *          How many threads to run.
   * @param brokerSleepMs
   *          How long each thread should sleep between batch writes.
   */
  public MockSystemConsumer(int messagesPerBatch, int threadCount, int brokerSleepMs) {
    super(new MetricsRegistryMap("test-container-performance"), new Clock() {
      @Override
      public long currentTimeMillis() {
        return System.currentTimeMillis();
      }
    });

    this.messagesPerBatch = messagesPerBatch;
    this.threadCount = threadCount;
    this.brokerSleepMs = brokerSleepMs;
    this.ssps = new HashSet<SystemStreamPartition>();
    this.threads = new ArrayList<Thread>(threadCount);
  }

  /**
   * Assign SystemStreamPartitions to all of the threads, and start them up to
   * begin simulating consuming messages.
   */
  @Override
  public void start() {
    for (int i = 0; i < threadCount; ++i) {
      Set<SystemStreamPartition> threadSsps = new HashSet<SystemStreamPartition>();

      // Assign SystemStreamPartitions for this thread.
      for (SystemStreamPartition ssp : ssps) {
        if (Math.abs(ssp.hashCode()) % threadCount == i) {
          threadSsps.add(ssp);
        }
      }

      // Start thread.
      Thread thread = new Thread(new MockSystemConsumerRunnable(threadSsps));
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }
  }

  /**
   * Kill all the threads, and shutdown.
   */
  @Override
  public void stop() {
    for (Thread thread : threads) {
      thread.interrupt();
    }

    try {
      for (Thread thread : threads) {
        thread.join();
      }
    } catch (InterruptedException e) {
    }
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String lastReadOffset) {
    super.register(systemStreamPartition, lastReadOffset);
    ssps.add(systemStreamPartition);
    setIsAtHead(systemStreamPartition, true);
  }

  /**
   * The worker thread for MockSystemConsumer that simulates reading messages
   * from a remote streaming system (i.e. Kafka), and writing them to the
   * BlockingEnvelopeMap.
   */
  public class MockSystemConsumerRunnable implements Runnable {
    private final Set<SystemStreamPartition> ssps;

    public MockSystemConsumerRunnable(Set<SystemStreamPartition> ssps) {
      this.ssps = ssps;
    }

    @Override
    public void run() {
      try {
        while (!Thread.interrupted() && ssps.size() > 0) {
          Set<SystemStreamPartition> sspsToFetch = new HashSet<SystemStreamPartition>();

          // Only fetch messages when there are no outstanding messages left.
          for (SystemStreamPartition ssp : ssps) {
            if (getNumMessagesInQueue(ssp) <= 0) {
              sspsToFetch.add(ssp);
            }
          }

          // Simulate a broker fetch request's network latency.
          Thread.sleep(brokerSleepMs);

          // Add messages to the BlockingEnvelopeMap.
          for (SystemStreamPartition ssp : sspsToFetch) {
            for (int i = 0; i < messagesPerBatch; ++i) {
              put(ssp, new IncomingMessageEnvelope(ssp, "0", "key", "value"));
            }
          }
        }
      } catch (InterruptedException e) {
        System.out.println("Got interrupt. Shutting down.");
      }
    }
  }
}
