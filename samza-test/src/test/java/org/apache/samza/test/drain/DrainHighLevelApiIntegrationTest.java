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
import net.jcip.annotations.NotThreadSafe;
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
import org.apache.samza.util.CoordinatorStreamUtil;
import org.junit.FixMethodOrder;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * End to end integration test to check drain functionality with samza high-level API.
 */
@FixMethodOrder
@NotThreadSafe
public class DrainHighLevelApiIntegrationTest {
  private static final List<PageView> RECEIVED = new ArrayList<>();
  private static final String SYSTEM_NAME1 = "test1";
  private static final String STREAM_ID1 = "PageView1";

  private static final String SYSTEM_NAME2 = "test2";
  private static final String STREAM_ID2 = "PageView2";

  /**
   * High-Level Job with multiple shuffle stages.
   * */
  private static class PageViewEventCountHighLevelApplication implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor(SYSTEM_NAME1);
      GenericInputDescriptor<KV<String, PageView>> isd =
          sd.getInputDescriptor(STREAM_ID1, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
      appDescriptor.getInputStream(isd)
          .map(KV::getValue)
          .partitionBy(PageView::getMemberId, pv -> pv,
              KVSerde.of(new IntegerSerde(), new TestTableData.PageViewJsonSerde()), "p11")
          .map(kv -> KV.of(kv.getKey() * 31, kv.getValue()))
          .partitionBy(KV::getKey, KV::getValue, KVSerde.of(new IntegerSerde(), new TestTableData.PageViewJsonSerde()), "p21")
          .sink((m, collector, coordinator) -> {
            RECEIVED.add(m.getValue());
          });
    }
  }

  /**
   * Simple high-level application without shuffle stages.
   * */
  private static class SimpleHighLevelApplication implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor(SYSTEM_NAME2);
      GenericInputDescriptor<KV<String, PageView>> isd =
          sd.getInputDescriptor(STREAM_ID2, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
      appDescriptor.getInputStream(isd)
          .sink((m, collector, coordinator) -> {
            RECEIVED.add(m.getValue());
          });
    }
  }


  /**
   * This test will test drain and consumption of some messages from the in-memory topic.
   * In order to simulate the real-world behaviour of drain, the test adds messages to the in-memory topic buffer periodically
   * in a delayed fashion instead of all at once. The test then writes the drain notification message to the in-memory
   * metadata store to drain and stop the pipeline. This write is done shortly after the pipeline starts and before all
   * the messages are written to the topic's buffer. As a result, the total count of the processed messages will be less
   * than the expected count of messages.
   * */
  @Test
  public void testDrain() {
    int numPageViews = 200;
    int numPartitions = 4;
    long delayBetweenMessagesInMillis = 500L;
    long drainTriggerDelay = 10_000L;
    String runId = "DrainTestId";

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME1);
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd.getInputDescriptor(STREAM_ID1, new NoOpSerde<>());
    InMemoryMetadataStoreFactory metadataStoreFactory = new InMemoryMetadataStoreFactory();

    Map<String, String> customConfig = ImmutableMap.of(
        ApplicationConfig.APP_RUN_ID, runId,
        JobConfig.DRAIN_MONITOR_POLL_INTERVAL_MILLIS, "100",
        JobConfig.DRAIN_MONITOR_ENABLED, "true");

    // Create a TestRunner
    // Set a InMemoryMetadataFactory.This factory is shared between TestRunner and DrainUtils's write drain method
    TestRunner testRunner = TestRunner.of(new PageViewEventCountHighLevelApplication())
        .setInMemoryMetadataFactory(metadataStoreFactory)
        .addConfig(customConfig)
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(numPageViews, numPartitions), delayBetweenMessagesInMillis);

    Config configFromRunner = testRunner.getConfig();
    MetadataStore metadataStore = metadataStoreFactory.getMetadataStore("NoOp", configFromRunner, new MetricsRegistryMap());

    // Write configs to the coordinator stream here as neither the passthrough JC nor the StreamProcessor is writing
    // configs to coordinator stream. RemoteApplicationRunner typically write the configs to the metadata store
    // before starting the JC.
    // We are doing this so that DrainUtils.writeDrainNotification can read app.run.id from the config
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(metadataStore, configFromRunner);

    // write drain message after a delay
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(new Callable<String>() {
      @Override
      public String call() throws Exception {
        UUID uuid = DrainUtils.writeDrainNotification(metadataStore);
        return uuid.toString();
      }
    }, drainTriggerDelay, TimeUnit.MILLISECONDS);

    testRunner.run(Duration.ofSeconds(40));

    assertTrue(RECEIVED.size() < numPageViews && RECEIVED.size() > 0);
    RECEIVED.clear();
    clearMetadataStore(metadataStore);
  }

  @Test
  public void testDrainWithoutReshuffleStages() {
    int numPageViews = 200;
    int numPartitions = 4;
    long delayBetweenMessagesInMillis = 500L;
    long drainTriggerDelay = 10_000L;
    String runId = "DrainTestId";

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME2);
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd.getInputDescriptor(STREAM_ID2, new NoOpSerde<>());
    InMemoryMetadataStoreFactory metadataStoreFactory = new InMemoryMetadataStoreFactory();

    Map<String, String> customConfig = ImmutableMap.of(
        ApplicationConfig.APP_RUN_ID, runId,
        JobConfig.DRAIN_MONITOR_POLL_INTERVAL_MILLIS, "100",
        JobConfig.DRAIN_MONITOR_ENABLED, "true");

    // Create a TestRunner
    // Set a InMemoryMetadataFactory.This factory is shared between TestRunner and DrainUtils's write drain method
    TestRunner testRunner = TestRunner.of(new SimpleHighLevelApplication())
        .setInMemoryMetadataFactory(metadataStoreFactory)
        .addConfig(customConfig)
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(numPageViews, numPartitions), delayBetweenMessagesInMillis);

    Config configFromRunner = testRunner.getConfig();
    MetadataStore metadataStore = metadataStoreFactory.getMetadataStore("NoOp", configFromRunner, new MetricsRegistryMap());

    // Write configs to the coordinator stream here as neither the passthrough JC nor the StreamProcessor is writing
    // configs to coordinator stream. RemoteApplicationRunner typically write the configs to the metadata store
    // before starting the JC.
    // We are doing this so that DrainUtils.writeDrainNotification can read app.run.id from the config
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(metadataStore, configFromRunner);

    // write drain message after a delay
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(new Callable<String>() {
      @Override
      public String call() throws Exception {
        UUID uuid = DrainUtils.writeDrainNotification(metadataStore);
        return uuid.toString();
      }
    }, drainTriggerDelay, TimeUnit.MILLISECONDS);

    testRunner.run(Duration.ofSeconds(40));

    assertTrue(RECEIVED.size() < numPageViews && RECEIVED.size() > 0);
    RECEIVED.clear();
    clearMetadataStore(metadataStore);
  }

  /**
   * This test will test drain and that no messages are processed as drain notification is written to the metadata store
   * before start.
   * */
  @Test
  public void testDrainOnContainerStart() {
    int numPageViews = 200;
    int numPartitions = 4;
    long delayBetweenMessagesInMillis = 500L;
    String runId = "DrainTestId";

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME1);
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd.getInputDescriptor(STREAM_ID1, new NoOpSerde<>());
    InMemoryMetadataStoreFactory metadataStoreFactory = new InMemoryMetadataStoreFactory();

    Map<String, String> customConfig = ImmutableMap.of(
        ApplicationConfig.APP_RUN_ID, runId,
        JobConfig.DRAIN_MONITOR_POLL_INTERVAL_MILLIS, "100",
        JobConfig.DRAIN_MONITOR_ENABLED, "true");

    // Create a TestRunner
    // Set a InMemoryMetadataFactory.This factory is shared between TestRunner and DrainUtils's write drain method
    TestRunner testRunner = TestRunner.of(new PageViewEventCountHighLevelApplication())
        .setInMemoryMetadataFactory(metadataStoreFactory)
        .addConfig(customConfig)
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(numPageViews, numPartitions), delayBetweenMessagesInMillis);

    Config configFromRunner = testRunner.getConfig();
    MetadataStore
        metadataStore = metadataStoreFactory.getMetadataStore("NoOp", configFromRunner, new MetricsRegistryMap());

    // Write configs to the coordinator stream here as neither the passthrough JC nor the StreamProcessor is writing
    // configs to coordinator stream. RemoteApplicationRunner typically write the configs to the metadata store
    // before starting the JC.
    // We are doing this so that DrainUtils.writeDrainNotification can read app.run.id from the config
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(metadataStore, configFromRunner);

    // write on the test thread to ensure that drain notification is available on container start
    DrainUtils.writeDrainNotification(metadataStore);

    testRunner.run(Duration.ofSeconds(20));

    assertEquals(RECEIVED.size(), 0);
    RECEIVED.clear();
    clearMetadataStore(metadataStore);
  }

  @Test
  public void testDrainOnContainerStartWithoutReshuffleStages() {
    int numPageViews = 200;
    int numPartitions = 4;
    long delayBetweenMessagesInMillis = 500L;
    String runId = "DrainTestId";

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME2);
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd.getInputDescriptor(STREAM_ID2, new NoOpSerde<>());
    InMemoryMetadataStoreFactory metadataStoreFactory = new InMemoryMetadataStoreFactory();

    Map<String, String> customConfig = ImmutableMap.of(
        ApplicationConfig.APP_RUN_ID, runId,
        JobConfig.DRAIN_MONITOR_POLL_INTERVAL_MILLIS, "100",
        JobConfig.DRAIN_MONITOR_ENABLED, "true");

    // Create a TestRunner
    // Set a InMemoryMetadataFactory.This factory is shared between TestRunner and DrainUtils's write drain method
    TestRunner testRunner = TestRunner.of(new SimpleHighLevelApplication())
        .setInMemoryMetadataFactory(metadataStoreFactory)
        .addConfig(customConfig)
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(numPageViews, numPartitions), delayBetweenMessagesInMillis);

    Config configFromRunner = testRunner.getConfig();
    MetadataStore
        metadataStore = metadataStoreFactory.getMetadataStore("NoOp", configFromRunner, new MetricsRegistryMap());

    // Write configs to the coordinator stream here as neither the passthrough JC nor the StreamProcessor is writing
    // configs to coordinator stream. RemoteApplicationRunner typically write the configs to the metadata store
    // before starting the JC.
    // We are doing this so that DrainUtils.writeDrainNotification can read app.run.id from the config
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(metadataStore, configFromRunner);

    // write on the test thread to ensure that drain notification is available on container start
    DrainUtils.writeDrainNotification(metadataStore);

    testRunner.run(Duration.ofSeconds(20));

    assertEquals(RECEIVED.size(), 0);
    RECEIVED.clear();
    clearMetadataStore(metadataStore);
  }

  private static void clearMetadataStore(MetadataStore store) {
    store.all().keySet().forEach(store::delete);
  }
}
