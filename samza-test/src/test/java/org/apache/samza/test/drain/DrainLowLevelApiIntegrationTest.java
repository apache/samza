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
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.context.Context;
import org.apache.samza.drain.DrainUtils;
import org.apache.samza.metadatastore.InMemoryMetadataStoreFactory;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.task.DrainListenerTask;
import org.apache.samza.task.EndOfStreamListenerTask;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.table.TestTableData;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.junit.FixMethodOrder;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * End to end integration test to check drain functionality with samza low-level API.
 * Tests have been annotated with @Ignore as they seem to timeout on the build system.
 * */
@NotThreadSafe
@FixMethodOrder
public class DrainLowLevelApiIntegrationTest {
  private static final List<TestTableData.PageView> RECEIVED = new ArrayList<>();
  private static Integer drainCounter = 0;
  private static Integer eosCounter = 0;

  private static final String SYSTEM_NAME = "test";
  private static final String STREAM_ID = "PageView";

  private static class PageViewEventCountLowLevelApplication implements TaskApplication {
    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor(SYSTEM_NAME);
      GenericInputDescriptor<TestTableData.PageView> pageViewIsd = ksd.getInputDescriptor(STREAM_ID, new NoOpSerde<>());
      appDescriptor
          .withInputStream(pageViewIsd)
          .withTaskFactory((StreamTaskFactory) PageViewEventCountStreamTask::new);
    }
  }

  private static class PageViewEventCountStreamTask implements StreamTask, InitableTask, DrainListenerTask, EndOfStreamListenerTask {
    public PageViewEventCountStreamTask() {
    }

    @Override
    public void init(Context context) {
    }

    @Override
    public void process(IncomingMessageEnvelope message, MessageCollector collector, TaskCoordinator coordinator) {
      TestTableData.PageView pv = (TestTableData.PageView) message.getMessage();
      RECEIVED.add(pv);
    }

    @Override
    public void onDrain(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
      drainCounter++;
    }

    @Override
    public void onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
      eosCounter++;
    }
  }

  /**
   * This test will test drain and consumption of some messages from the in-memory topic.
   * In order to simulate the real-world behaviour of drain, the test adds messages to the in-memory topic buffer in
   * a delayed fashion instead of all at once. The test then writes the drain notification message to the in-memory
   * metadata store to drain and stop the pipeline. This write is done shortly after the pipeline starts and before all
   * the messages are written to the topic's buffer. As a result, the total count of the processed messages will be less
   * than the expected count of messages.
   * */
  @Test
  public void testDrain() {
    int numPageViews = 200;
    int numPartitions = 4;
    long delayBetweenMessagesInMillis = 500L;
    long drainTriggerDelay = 5000L;
    String runId = "DrainTestId";

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd.getInputDescriptor("PageView", new NoOpSerde<>());
    InMemoryMetadataStoreFactory metadataStoreFactory = new InMemoryMetadataStoreFactory();

    Map<String, String> customConfig = ImmutableMap.of(
        ApplicationConfig.APP_RUN_ID, runId,
        JobConfig.DRAIN_MONITOR_POLL_INTERVAL_MILLIS, "100",
        JobConfig.DRAIN_MONITOR_ENABLED, "true");

    // Create a TestRunner
    // Set a InMemoryMetadataFactory.This factory is shared between TestRunner and DrainUtils's write drain method
    TestRunner testRunner = TestRunner.of(new PageViewEventCountLowLevelApplication())
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

    // Trigger drain after a delay
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
  public void testDrainOnContainerStart() {
    int numPageViews = 200;
    int numPartitions = 4;
    long delayBetweenMessagesInMillis = 500L;
    String runId = "DrainTestId";

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd.getInputDescriptor("PageView", new NoOpSerde<>());
    InMemoryMetadataStoreFactory metadataStoreFactory = new InMemoryMetadataStoreFactory();

    Map<String, String> customConfig = ImmutableMap.of(
        ApplicationConfig.APP_RUN_ID, runId,
        JobConfig.DRAIN_MONITOR_POLL_INTERVAL_MILLIS, "100",
        JobConfig.DRAIN_MONITOR_ENABLED, "true");

    // Create a TestRunner
    // Set a InMemoryMetadataFactory.This factory is shared between TestRunner and DrainUtils's write drain method
    TestRunner testRunner = TestRunner.of(new PageViewEventCountLowLevelApplication())
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

    testRunner.run(Duration.ofSeconds(40));

    assertEquals(RECEIVED.size(), 0);
    RECEIVED.clear();
    clearMetadataStore(metadataStore);
  }

  private static void clearMetadataStore(MetadataStore store) {
    store.all().keySet().forEach(store::delete);
  }
}
