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

package org.apache.samza.metrics;

import java.util.List;
import java.util.Map;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.metrics.reporter.MetricsSnapshotReporter;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.serializers.Serializer;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import scala.Some;
import scala.runtime.AbstractFunction0;

import static org.mockito.Mockito.*;


public class TestMetricsSnapshotReporter {

  private MetricsSnapshotReporter metricsSnapshotReporter;
  private static final String BLACKLIST_ALL = ".*";
  private static final String BLACKLIST_NONE = "";
  private static final String BLACKLIST_GROUPS = ".*(SystemConsumersMetrics|CachedStoreMetrics).*";
  private static final String BLACKLIST_ALL_BUT_TWO_GROUPS = "^(?!.*?(?:SystemConsumersMetrics|CachedStoreMetrics)).*$";

  private static final SystemStream SYSTEM_STREAM = new SystemStream("test system", "test stream");
  private static final String JOB_NAME = "test job";
  private static final String JOB_ID = "test jobID";
  private static final String CONTAINER_NAME = "samza-container-0";
  private static final String TASK_VERSION = "test version";
  private static final String SAMZA_VERSION = "test samza version";
  private static final String HOSTNAME = "test host";
  private static final int REPORTING_INTERVAL = 60000;

  private Serializer<MetricsSnapshot> SERIALIZER;
  private SystemProducer PRODUCER;

  @Before
  public void setup() {
    PRODUCER = mock(SystemProducer.class);
    SERIALIZER = new MetricsSnapshotSerdeV2();
  }

  @Test
  public void testBlacklistAll() {
    this.metricsSnapshotReporter = getMetricsSnapshotReporter(BLACKLIST_ALL);

    Assert.assertTrue("Should ignore all metrics",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.kafka.KafkaSystemProducerMetrics",
            "kafka-flush-ns"));

    Assert.assertTrue("Should ignore all metrics",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.storage.kv.LoggedStoreMetrics", "stats-ranges"));

    Assert.assertTrue("Should ignore all metrics",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.SystemProducersMetrics", "flushes"));
  }

  @Test
  public void testBlacklistNone() {
    this.metricsSnapshotReporter = getMetricsSnapshotReporter(BLACKLIST_NONE);

    Assert.assertFalse("Should not ignore any metrics",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.kafka.KafkaSystemProducerMetrics",
            "kafka-flush-ns"));

    Assert.assertFalse("Should not ignore any metrics",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.storage.kv.LoggedStoreMetrics", "stats-ranges"));

    Assert.assertFalse("Should not ignore any metrics",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.SystemProducersMetrics", "flushes"));
  }

  @Test
  public void testBlacklistGroup() {
    this.metricsSnapshotReporter = getMetricsSnapshotReporter(BLACKLIST_GROUPS);
    Assert.assertTrue("Should ignore all metrics from this group",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.SystemConsumersMetrics", "poll-ns"));

    Assert.assertTrue("Should ignore all metrics from this group",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.SystemConsumersMetrics",
            "unprocessed-messages"));

    Assert.assertTrue("Should ignore all metrics from this group",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.storage.kv.CachedStoreMetrics",
            "storename-stats-flushes"));

    Assert.assertFalse("Should not ignore any other group",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics",
            "poll-count"));
  }

  @Test
  public void testBlacklistAllButTwoGroups() {
    this.metricsSnapshotReporter = getMetricsSnapshotReporter(BLACKLIST_ALL_BUT_TWO_GROUPS);

    Assert.assertFalse("Should not ignore this group",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.SystemConsumersMetrics", "poll-ns"));

    Assert.assertFalse("Should not ignore this group",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.storage.kv.CachedStoreMetrics",
            "storename-stats-flushes"));

    Assert.assertTrue("Should ignore all metrics from any other groups",
        this.metricsSnapshotReporter.shouldIgnore("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics",
            "poll-count"));
  }

  @Test
  public void testMetricsEmission() {
    // setup
    SERIALIZER = null;
    String SOURCE = "testSource";
    String GROUP = "someGroup";
    String METRIC_NAME = "someName";
    MetricsRegistryMap registry = new MetricsRegistryMap();

    metricsSnapshotReporter = getMetricsSnapshotReporter(TestMetricsSnapshotReporter.BLACKLIST_NONE);
    registry.newGauge(GROUP, METRIC_NAME, 42);
    metricsSnapshotReporter.register(SOURCE, registry);

    ArgumentCaptor<OutgoingMessageEnvelope> outgoingMessageEnvelopeArgumentCaptor =
        ArgumentCaptor.forClass(OutgoingMessageEnvelope.class);

    // run
    metricsSnapshotReporter.run();

    // assert
    verify(PRODUCER, times(1)).send(eq(SOURCE), outgoingMessageEnvelopeArgumentCaptor.capture());
    verify(PRODUCER, times(1)).flush(eq(SOURCE));

    List<OutgoingMessageEnvelope> envelopes = outgoingMessageEnvelopeArgumentCaptor.getAllValues();

    Assert.assertEquals(1, envelopes.size());

    MetricsSnapshot metricsSnapshot = ((MetricsSnapshot) envelopes.get(0).getMessage());

    Assert.assertEquals(JOB_NAME, metricsSnapshot.getHeader().getJobName());
    Assert.assertEquals(JOB_ID, metricsSnapshot.getHeader().getJobId());
    Assert.assertEquals(CONTAINER_NAME, metricsSnapshot.getHeader().getContainerName());
    Assert.assertEquals(SOURCE, metricsSnapshot.getHeader().getSource());
    Assert.assertEquals(SAMZA_VERSION, metricsSnapshot.getHeader().getSamzaVersion());
    Assert.assertEquals(TASK_VERSION, metricsSnapshot.getHeader().getVersion());
    Assert.assertEquals(HOSTNAME, metricsSnapshot.getHeader().getHost());

    Map<String, Map<String, Object>> metricMap = metricsSnapshot.getMetrics().getAsMap();
    Assert.assertEquals(1, metricMap.size());
    Assert.assertTrue(metricMap.containsKey(GROUP));
    Assert.assertTrue(metricMap.get(GROUP).containsKey(METRIC_NAME));
    Assert.assertEquals(42, metricMap.get(GROUP).get(METRIC_NAME));
  }

  private MetricsSnapshotReporter getMetricsSnapshotReporter(String blacklist) {
    return new MetricsSnapshotReporter(PRODUCER, SYSTEM_STREAM, REPORTING_INTERVAL, JOB_NAME, JOB_ID, CONTAINER_NAME,
        TASK_VERSION, SAMZA_VERSION, HOSTNAME, SERIALIZER, new Some<>(blacklist), getClock());
  }

  private AbstractFunction0<Object> getClock() {
    return new AbstractFunction0<Object>() {
      @Override
      public Object apply() {
        return System.currentTimeMillis();
      }
    };
  }
}
