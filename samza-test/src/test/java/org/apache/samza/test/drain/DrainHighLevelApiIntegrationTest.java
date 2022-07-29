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

package org.apache.samza.test.drain;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.drain.DrainUtils;
import org.apache.samza.metadatastore.InMemoryMetadataStoreFactory;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.table.TestTableData;
import org.apache.samza.test.table.TestTableData.PageView;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * End to end integration test to check drain functionality with samza high-level API.
 */
public class DrainHighLevelApiIntegrationTest {
  private static final List<PageView> RECEIVED = new ArrayList<>();
  private static final String SYSTEM_NAME = "test";
  private static final String STREAM_ID = "PageView";

  private static class  PageViewEventCountHighLevelApplication implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor(SYSTEM_NAME);
      GenericInputDescriptor<KV<String, PageView>> isd =
          sd.getInputDescriptor(STREAM_ID, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
      appDescriptor.getInputStream(isd)
          .map(KV::getValue)
          .partitionBy(PageView::getMemberId, pv -> pv,
              KVSerde.of(new IntegerSerde(), new TestTableData.PageViewJsonSerde()), "p1")
          .sink((m, collector, coordinator) -> {
            RECEIVED.add(m.getValue());
          });
    }
  }

  // The test can be occasionally flaky, so we set Ignore annotation
  // Remove ignore annotation and run the test as follows:
  // ./gradlew :samza-test:test --tests org.apache.samza.test.drain.DrainHighLevelApiIntegrationTest -PscalaSuffix=2.12
  @Ignore
  @Test
  public void testPipeline() {
    String runId = "DrainTestId";
    int numPageViews = 40;

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME);
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd.getInputDescriptor(STREAM_ID, new NoOpSerde<>());
    InMemoryMetadataStoreFactory metadataStoreFactory = new InMemoryMetadataStoreFactory();

    Map<String, String> customConfig = ImmutableMap.of(
        ApplicationConfig.APP_RUN_ID, runId,
        JobConfig.DRAIN_MONITOR_POLL_INTERVAL_MILLIS, "100",
        JobConfig.DRAIN_MONITOR_ENABLED, "true");

    // Create a TestRunner
    // Set a InMemoryMetadataFactory. We will use this factory in the test to create a metadata store and
    // write drain message to it
    // Mock data comprises of 40 messages across 4 partitions. TestRunner adds a 1 second delay between messages
    // per partition when writing messages to the InputStream
    TestRunner testRunner = TestRunner.of(new PageViewEventCountHighLevelApplication())
        .setInMemoryMetadataFactory(metadataStoreFactory)
        .addConfig(customConfig)
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(numPageViews, 4), 1000L);

    Config configFromRunner = testRunner.getConfig();
    MetadataStore metadataStore = metadataStoreFactory.getMetadataStore("NoOp", configFromRunner, new MetricsRegistryMap());

    // write drain message after a delay
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(new Callable<String>() {
      @Override
      public String call() throws Exception {
        UUID uuid = DrainUtils.writeDrainNotification(metadataStore, runId);
        return uuid.toString();
      }
    }, 2000L, TimeUnit.MILLISECONDS);

    testRunner.run(Duration.ofSeconds(20));

    assertTrue(RECEIVED.size() < numPageViews && RECEIVED.size() > 0);
  }
}
