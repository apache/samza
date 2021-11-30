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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.serializers.Serializer;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.SystemClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


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
  private static final Duration REPORTING_INTERVAL = Duration.ofSeconds(60000);

  private Serializer<MetricsSnapshot> serializer;
  private SystemProducer producer;

  @Before
  public void setup() {
    producer = mock(SystemProducer.class);
    serializer = new MetricsSnapshotSerdeV2();
  }

  @Test
  public void testBlacklistAll() {
    this.metricsSnapshotReporter = getMetricsSnapshotReporter(Optional.of(BLACKLIST_ALL));

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
    this.metricsSnapshotReporter = getMetricsSnapshotReporter(Optional.of(BLACKLIST_NONE));

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
    this.metricsSnapshotReporter = getMetricsSnapshotReporter(Optional.of(BLACKLIST_GROUPS));
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
    this.metricsSnapshotReporter = getMetricsSnapshotReporter(Optional.of(BLACKLIST_ALL_BUT_TWO_GROUPS));

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
    serializer = null;
    String source = "testSource";
    String group = "someGroup";
    String metricName = "someName";
    MetricsRegistryMap registry = new MetricsRegistryMap();

    metricsSnapshotReporter = getMetricsSnapshotReporter(Optional.empty());
    registry.newGauge(group, metricName, 42);
    metricsSnapshotReporter.register(source, registry);

    ArgumentCaptor<OutgoingMessageEnvelope> outgoingMessageEnvelopeArgumentCaptor =
        ArgumentCaptor.forClass(OutgoingMessageEnvelope.class);

    // run
    metricsSnapshotReporter.run();

    // assert
    verify(producer, times(1)).send(eq(source), outgoingMessageEnvelopeArgumentCaptor.capture());
    verify(producer, times(1)).flush(eq(source));

    List<OutgoingMessageEnvelope> envelopes = outgoingMessageEnvelopeArgumentCaptor.getAllValues();

    Assert.assertEquals(1, envelopes.size());

    MetricsSnapshot metricsSnapshot = (MetricsSnapshot) envelopes.get(0).getMessage();

    Assert.assertEquals(JOB_NAME, metricsSnapshot.getHeader().getJobName());
    Assert.assertEquals(JOB_ID, metricsSnapshot.getHeader().getJobId());
    Assert.assertEquals(CONTAINER_NAME, metricsSnapshot.getHeader().getContainerName());
    Assert.assertEquals(source, metricsSnapshot.getHeader().getSource());
    Assert.assertEquals(SAMZA_VERSION, metricsSnapshot.getHeader().getSamzaVersion());
    Assert.assertEquals(TASK_VERSION, metricsSnapshot.getHeader().getVersion());
    Assert.assertEquals(HOSTNAME, metricsSnapshot.getHeader().getHost());

    Map<String, Map<String, Object>> metricMap = metricsSnapshot.getMetrics().getAsMap();
    Assert.assertEquals(1, metricMap.size());
    Assert.assertTrue(metricMap.containsKey(group));
    Assert.assertTrue(metricMap.get(group).containsKey(metricName));
    Assert.assertEquals(42, metricMap.get(group).get(metricName));
  }

  private MetricsSnapshotReporter getMetricsSnapshotReporter(Optional<String> blacklist) {
    return new MetricsSnapshotReporter(producer, SYSTEM_STREAM, REPORTING_INTERVAL, JOB_NAME, JOB_ID, CONTAINER_NAME,
        TASK_VERSION, SAMZA_VERSION, HOSTNAME, serializer, blacklist.map(Pattern::compile), SystemClock.instance());
  }
}
