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

package org.apache.samza.coordinator.stream;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;
import org.apache.samza.util.Util;

import java.util.ArrayList;
import java.util.List;


/**
 * Helper for creating mock CoordinatorStreamConsumer and
 * CoordinatorStreamConsumer. The CoordinatorStreamConsumer is meant to just
 * forward all configs to JobCoordinator, which is useful for mocking in unit
 * tests.
 */
public class MockCoordinatorStreamSystemFactory implements SystemFactory {

  private static SystemConsumer mockConsumer = null;
  private static boolean useCachedConsumer = false;

  public static void enableMockConsumerCache() {
    mockConsumer = null;
    useCachedConsumer = true;
  }

  public static void disableMockConsumerCache() {
    useCachedConsumer = false;
    mockConsumer = null;
  }

  /**
   * Returns a consumer that sends all configs to the coordinator stream.
   *
   * @param config Along with the configs, you can pass checkpoints and changelog stream messages into the stream.
   *               The expected pattern is cp:source:taskname -> ssp,offset for checkpoint (Use sspToString util)
   *               ch:source:taskname -> changelogPartition for changelog
   *               Everything else is processed as normal config
   */
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {

    if (useCachedConsumer && mockConsumer != null) {
      return mockConsumer;
    }

    String jobName = config.get("job.name");
    String jobId = config.get("job.id");
    if (jobName == null) {
      throw new ConfigException("Must define job.name.");
    }
    if (jobId == null) {
      jobId = "1";
    }
    String streamName = Util.getCoordinatorStreamName(jobName, jobId);
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, streamName, new Partition(0));
    mockConsumer = new MockCoordinatorStreamWrappedConsumer(systemStreamPartition, config);
    return mockConsumer;
  }

  /**
   * Returns a MockCoordinatorSystemProducer.
   */
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new MockSystemProducer(null);
  }

  /**
   * Returns a single partition admin that pretends to create a coordinator
   * stream.
   */
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new MockSystemAdmin();
  }

  public static final class MockSystemAdmin extends SinglePartitionWithoutOffsetsSystemAdmin implements SystemAdmin {
    public void createCoordinatorStream(String streamName) {
      // Do nothing.
    }
  }

  protected static class MockSystemProducer implements SystemProducer {
    private final String expectedSource;
    private final List<OutgoingMessageEnvelope> envelopes;
    private boolean started = false;
    private boolean registered = false;
    private boolean flushed = false;

    public MockSystemProducer(String expectedSource) {
      this.expectedSource = expectedSource;
      this.envelopes = new ArrayList<OutgoingMessageEnvelope>();
    }


    public void start() {
      started = true;
    }

    public void stop() {
      started = false;
    }

    public void register(String source) {
      registered = true;
    }

    public void send(String source, OutgoingMessageEnvelope envelope) {
      envelopes.add(envelope);
    }

    public void flush(String source) {
      flushed = true;
    }

    public List<OutgoingMessageEnvelope> getEnvelopes() {
      return envelopes;
    }

    public boolean isStarted() {
      return started;
    }

    public boolean isStopped() {
      return !started;
    }

    public boolean isRegistered() {
      return registered;
    }

    public boolean isFlushed() {
      return flushed;
    }

    public String getExpectedSource() {
      return expectedSource;
    }
  }
}
