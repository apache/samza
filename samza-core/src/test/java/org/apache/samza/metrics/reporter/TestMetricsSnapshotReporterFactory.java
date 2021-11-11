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
package org.apache.samza.metrics.reporter;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class TestMetricsSnapshotReporterFactory {
  private static final String REPORTER = "metrics-reporter";
  private static final SystemProducer SYSTEM_PRODUCER = mock(SystemProducer.class);
  private static final Serde<MetricsSnapshot> SERDE = mock(Serde.class);

  private MetricsSnapshotReporterFactory factory;

  @Before
  public void setup() {
    this.factory = new MetricsSnapshotReporterFactory();
  }

  @Test
  public void testGetProducer() {
    Config config = new MapConfig(
        ImmutableMap.of("metrics.reporter.metrics-reporter.stream", "system0.stream0", "systems.system0.samza.factory",
            MockSystemFactory.class.getName()));
    assertEquals(SYSTEM_PRODUCER, this.factory.getProducer(REPORTER, config, new MetricsRegistryMap()));
  }

  @Test
  public void testGetSystemStream() {
    Config config = new MapConfig(ImmutableMap.of("metrics.reporter.metrics-reporter.stream", "system0.stream0"));
    assertEquals(new SystemStream("system0", "stream0"), this.factory.getSystemStream(REPORTER, config));
  }

  @Test
  public void testGetSerdeConfigured() {
    Config config = new MapConfig(
        ImmutableMap.of("metrics.reporter.metrics-reporter.stream", "system0.stream0", "streams.stream0.samza.system",
            "system0", "streams.stream0.samza.msg.serde", "snapshot-serde", "serializers.registry.snapshot-serde.class",
            MockSerdeFactory.class.getName()));
    assertEquals(SERDE, this.factory.getSerde(REPORTER, config));
  }

  @Test
  public void testGetSerdeNoSerdeFactory() {
    Config config = new MapConfig(
        ImmutableMap.of("metrics.reporter.metrics-reporter.stream", "system0.stream0", "streams.stream0.samza.system",
            "system0", "streams.stream0.samza.msg.serde", "snapshot-serde"));
    assertNull(this.factory.getSerde(REPORTER, config));
  }

  @Test
  public void testGetSerdeFallback() {
    Config config = new MapConfig(
        ImmutableMap.of("metrics.reporter.metrics-reporter.stream", "system0.stream0", "streams.stream0.samza.system",
            "system0"));
    assertTrue(this.factory.getSerde(REPORTER, config) instanceof MetricsSnapshotSerdeV2);
  }

  public static class MockSystemFactory implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
      throw new UnsupportedOperationException("Unnecessary for test");
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
      return SYSTEM_PRODUCER;
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
      throw new UnsupportedOperationException("Unnecessary for test");
    }
  }

  public static class MockSerdeFactory implements SerdeFactory<MetricsSnapshot> {
    @Override
    public Serde<MetricsSnapshot> getSerde(String name, Config config) {
      return SERDE;
    }
  }
}