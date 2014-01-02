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

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

/**
 * MockSystemFactory was built to make performance testing easier.
 */
public class MockSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    MockSystemConsumerConfig consumerConfig = new MockSystemConsumerConfig(systemName, config);

    return new MockSystemConsumer(consumerConfig.getMessagesPerBatch(), consumerConfig.getConsumerThreadCount(), consumerConfig.getBrokerSleepMs());
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    throw new RuntimeException("MockSystemProducer not implemented.");
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    MockSystemConsumerConfig consumerConfig = new MockSystemConsumerConfig(systemName, config);

    return new MockSystemAdmin(consumerConfig.getPartitionsPerStream());
  }

  /**
   * A helper class that's useful for yanking out MockSystem's configuration
   * out.
   */
  public static class MockSystemConsumerConfig {
    public static final int DEFAULT_PARTITION_COUNT = 4;
    public static final int DEFAULT_MESSAGES_PER_BATCH = 5000;
    public static final int DEFAULT_CONSUMER_THREAD_COUNT = 12;
    public static final int DEFAULT_BROKER_SLEEP_MS = 1;

    private final String systemName;
    private final Config config;

    public MockSystemConsumerConfig(String systemName, Config config) {
      this.systemName = systemName;
      this.config = config;
    }

    /**
     * @return the partition count to be used for MockSystemAdmin.
     */
    public int getPartitionsPerStream() {
      return config.getInt("systems." + systemName + ".partitions.per.stream", DEFAULT_PARTITION_COUNT);
    }

    /**
     * @return the messages per batch to be used for the MockSystemConsumer.
     */
    public int getMessagesPerBatch() {
      return config.getInt("systems." + systemName + ".messages.per.batch", DEFAULT_MESSAGES_PER_BATCH);
    }

    /**
     * @return the number of threads to be used for the MockSystemConsumer.
     */
    public int getConsumerThreadCount() {
      return config.getInt("systems." + systemName + ".consumer.thread.count", DEFAULT_CONSUMER_THREAD_COUNT);
    }

    /**
     * @return the milliseconds to sleep between each batch in
     *         MockSystemConsumer.
     */
    public int getBrokerSleepMs() {
      return config.getInt("systems." + systemName + ".broker.sleep.ms", DEFAULT_BROKER_SLEEP_MS);
    }
  }
}
