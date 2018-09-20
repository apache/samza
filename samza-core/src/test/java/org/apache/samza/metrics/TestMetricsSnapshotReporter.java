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

import org.apache.samza.metrics.reporter.MetricsSnapshotReporter;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.inmemory.InMemorySystemProducer;
import org.junit.Assert;
import org.junit.Test;
import scala.Some;
import scala.runtime.AbstractFunction0;


public class TestMetricsSnapshotReporter {
  private MetricsSnapshotReporter metricsSnapshotReporter;
  private static final String BLACKLIST_ALL = ".*";
  private static final String BLACKLIST_NONE = "";
  private static final String BLACKLIST_GROUPS = ".*(SystemConsumersMetrics|CachedStoreMetrics).*";
  private static final String BLACKLIST_ALL_BUT_TWO_GROUPS = "^(?!.*?(?:SystemConsumersMetrics|CachedStoreMetrics)).*$";

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

  private MetricsSnapshotReporter getMetricsSnapshotReporter(String blacklist) {
    return new MetricsSnapshotReporter(new InMemorySystemProducer("test system", null),
        new SystemStream("test system", "test stream"), 60000, "test job", "test jobID", "samza-container-0",
        "test version", "test samza version", "test host", new MetricsSnapshotSerdeV2(), new Some<>(blacklist),
        new AbstractFunction0<Object>() {
          @Override
          public Object apply() {
            return System.currentTimeMillis();
          }
        });
  }
}
