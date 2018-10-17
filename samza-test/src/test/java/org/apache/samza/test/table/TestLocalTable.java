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

package org.apache.samza.test.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.context.Context;
import org.apache.samza.context.TaskContext;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.LocalStoreBackedReadWriteTable;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.ArraySystemFactory;
import org.apache.samza.test.util.Base64Serializer;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.EnrichedPageView;
import static org.apache.samza.test.table.TestTableData.PageView;
import static org.apache.samza.test.table.TestTableData.PageViewJsonSerde;
import static org.apache.samza.test.table.TestTableData.PageViewJsonSerdeFactory;
import static org.apache.samza.test.table.TestTableData.Profile;
import static org.apache.samza.test.table.TestTableData.ProfileJsonSerde;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * This test class tests sendTo() and join() for local tables
 */
public class TestLocalTable extends AbstractIntegrationTestHarness {

  @Test
  public void testSendTo() throws  Exception {

    int count = 10;
    Profile[] profiles = TestTableData.generateProfiles(count);

    int partitionCount = 4;
    Map<String, String> configs = getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.Profile.samza.system", "test");
    configs.put("streams.Profile.source", Base64Serializer.serialize(profiles));
    configs.put("streams.Profile.partitionCount", String.valueOf(partitionCount));

    MyMapFunction mapFn = new MyMapFunction();

    final StreamApplication app = appDesc -> {

      Table<KV<Integer, Profile>> table = appDesc.getTable(new InMemoryTableDescriptor("t1",
          KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())));
      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      GenericInputDescriptor<Profile> isd = ksd.getInputDescriptor("Profile", new NoOpSerde<>());

      appDesc.getInputStream(isd)
          .map(mapFn)
          .sendTo(table);
    };

    final LocalApplicationRunner runner = new LocalApplicationRunner(app, new MapConfig(configs));
    runner.run();
    runner.waitForFinish();

    for (int i = 0; i < partitionCount; i++) {
      MyMapFunction mapFnCopy = MyMapFunction.getMapFunctionByTask(String.format("Partition %d", i));
      assertEquals(count, mapFnCopy.received.size());
      mapFnCopy.received.forEach(p -> Assert.assertTrue(mapFnCopy.table.get(p.getMemberId()) != null));
    }
  }

  static class TestStreamTableJoin {
    static List<PageView> received = new LinkedList<>();
    static List<EnrichedPageView> joined = new LinkedList<>();
    final int count;
    final int partitionCount;
    final Map<String, String> configs;

    TestStreamTableJoin(int count, int partitionCount, Map<String, String> configs) {
      this.count = count;
      this.partitionCount = partitionCount;
      this.configs = configs;
    }

    void runTest() {
      final StreamApplication app = appDesc -> {

        Table<KV<Integer, Profile>> table = appDesc.getTable(
            new InMemoryTableDescriptor("t1", KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())));
        DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
        GenericInputDescriptor<Profile> profileISD = ksd.getInputDescriptor("Profile", new NoOpSerde<>());
        appDesc.getInputStream(profileISD)
            .map(m -> new KV(m.getMemberId(), m))
            .sendTo(table);

        GenericInputDescriptor<PageView> pageViewISD = ksd.getInputDescriptor("PageView", new NoOpSerde<>());
        appDesc.getInputStream(pageViewISD)
            .map(pv -> {
                received.add(pv);
                return pv;
              })
            .partitionBy(PageView::getMemberId, v -> v, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()), "p1")
            .join(table, new PageViewToProfileJoinFunction())
            .sink((m, collector, coordinator) -> joined.add(m));
      };

      final LocalApplicationRunner runner = new LocalApplicationRunner(app, new MapConfig(configs));
      runner.run();
      runner.waitForFinish();

      assertEquals(count * partitionCount, received.size());
      assertEquals(count * partitionCount, joined.size());
      assertTrue(joined.get(0) instanceof EnrichedPageView);
    }
  }

  @Test
  public void testStreamTableJoin() throws Exception {

    int count = 10;
    PageView[] pageViews = TestTableData.generatePageViews(count);
    Profile[] profiles = TestTableData.generateProfiles(count);

    int partitionCount = 4;
    Map<String, String> configs = getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    configs.put("streams.Profile.samza.system", "test");
    configs.put("streams.Profile.samza.bootstrap", "true");
    configs.put("streams.Profile.source", Base64Serializer.serialize(profiles));
    configs.put("streams.Profile.partitionCount", String.valueOf(partitionCount));

    TestStreamTableJoin joinTest = new TestStreamTableJoin(count, partitionCount, configs);
    joinTest.runTest();
  }

  static class TestDualStreamTableJoin {
    static List<Profile> sentToProfileTable1 = new LinkedList<>();
    static List<Profile> sentToProfileTable2 = new LinkedList<>();
    static List<EnrichedPageView> joinedPageViews1 = new LinkedList<>();
    static List<EnrichedPageView> joinedPageViews2 = new LinkedList<>();
    final int count;
    final int partitionCount;
    final Map<String, String> configs;

    TestDualStreamTableJoin(int count, int partitionCount, Map<String, String> configs) {
      this.count = count;
      this.partitionCount = partitionCount;
      this.configs = configs;
    }

    void runTest() {
      KVSerde<Integer, Profile> profileKVSerde = KVSerde.of(new IntegerSerde(), new ProfileJsonSerde());
      KVSerde<Integer, PageView> pageViewKVSerde = KVSerde.of(new IntegerSerde(), new PageViewJsonSerde());

      PageViewToProfileJoinFunction joinFn1 = new PageViewToProfileJoinFunction();
      PageViewToProfileJoinFunction joinFn2 = new PageViewToProfileJoinFunction();

      final StreamApplication app = appDesc -> {

        Table<KV<Integer, Profile>> profileTable = appDesc.getTable(new InMemoryTableDescriptor("t1", profileKVSerde));

        DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
        GenericInputDescriptor<Profile> profileISD1 = ksd.getInputDescriptor("Profile1", new NoOpSerde<>());
        GenericInputDescriptor<Profile> profileISD2 = ksd.getInputDescriptor("Profile2", new NoOpSerde<>());
        MessageStream<Profile> profileStream1 = appDesc.getInputStream(profileISD1);
        MessageStream<Profile> profileStream2 = appDesc.getInputStream(profileISD2);

        profileStream1
            .map(m -> {
                sentToProfileTable1.add(m);
                return new KV(m.getMemberId(), m);
              })
            .sendTo(profileTable);
        profileStream2
            .map(m -> {
                sentToProfileTable2.add(m);
                return new KV(m.getMemberId(), m);
              })
            .sendTo(profileTable);

        GenericInputDescriptor<PageView> pageViewISD1 = ksd.getInputDescriptor("PageView1", new NoOpSerde<PageView>());
        GenericInputDescriptor<PageView> pageViewISD2 = ksd.getInputDescriptor("PageView2", new NoOpSerde<PageView>());
        MessageStream<PageView> pageViewStream1 = appDesc.getInputStream(pageViewISD1);
        MessageStream<PageView> pageViewStream2 = appDesc.getInputStream(pageViewISD2);

        pageViewStream1
            .partitionBy(PageView::getMemberId, v -> v, pageViewKVSerde, "p1")
            .join(profileTable, joinFn1)
            .sink((m, collector, coordinator) -> joinedPageViews1.add(m));

        pageViewStream2
            .partitionBy(PageView::getMemberId, v -> v, pageViewKVSerde, "p2")
            .join(profileTable, joinFn2)
            .sink((m, collector, coordinator) -> joinedPageViews2.add(m));
      };

      final LocalApplicationRunner runner = new LocalApplicationRunner(app, new MapConfig(configs));
      runner.run();
      runner.waitForFinish();

      assertEquals(count * partitionCount, sentToProfileTable1.size());
      assertEquals(count * partitionCount, sentToProfileTable2.size());

      assertEquals(count * partitionCount, joinedPageViews1.size());
      assertEquals(count * partitionCount, joinedPageViews2.size());
      assertTrue(joinedPageViews1.get(0) instanceof EnrichedPageView);
      assertTrue(joinedPageViews2.get(0) instanceof EnrichedPageView);

    }

  }

  @Test
  public void testDualStreamTableJoin() throws Exception {

    int count = 10;
    PageView[] pageViews = TestTableData.generatePageViews(count);
    Profile[] profiles = TestTableData.generateProfiles(count);

    int partitionCount = 4;
    Map<String, String> configs = getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.Profile1.samza.system", "test");
    configs.put("streams.Profile1.source", Base64Serializer.serialize(profiles));
    configs.put("streams.Profile1.samza.bootstrap", "true");
    configs.put("streams.Profile1.partitionCount", String.valueOf(partitionCount));

    configs.put("streams.Profile2.samza.system", "test");
    configs.put("streams.Profile2.source", Base64Serializer.serialize(profiles));
    configs.put("streams.Profile2.samza.bootstrap", "true");
    configs.put("streams.Profile2.partitionCount", String.valueOf(partitionCount));

    configs.put("streams.PageView1.samza.system", "test");
    configs.put("streams.PageView1.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView1.partitionCount", String.valueOf(partitionCount));

    configs.put("streams.PageView2.samza.system", "test");
    configs.put("streams.PageView2.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView2.partitionCount", String.valueOf(partitionCount));

    TestDualStreamTableJoin dualJoinTest = new TestDualStreamTableJoin(count, partitionCount, configs);
    dualJoinTest.runTest();
  }

  static Map<String, String> getBaseJobConfig(String bootstrapUrl, String zkConnect) {
    Map<String, String> configs = new HashMap<>();
    configs.put("systems.test.samza.factory", ArraySystemFactory.class.getName());

    configs.put(JobConfig.JOB_NAME(), "test-table-job");
    configs.put(JobConfig.PROCESSOR_ID(), "1");
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    // For intermediate streams
    configs.put("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems.kafka.producer.bootstrap.servers", bootstrapUrl);
    configs.put("systems.kafka.consumer.zookeeper.connect", zkConnect);
    configs.put("systems.kafka.samza.key.serde", "int");
    configs.put("systems.kafka.samza.msg.serde", "json");
    configs.put("systems.kafka.default.stream.replication.factor", "1");
    configs.put("job.default.system", "kafka");

    configs.put("serializers.registry.int.class", "org.apache.samza.serializers.IntegerSerdeFactory");
    configs.put("serializers.registry.json.class", PageViewJsonSerdeFactory.class.getName());

    return configs;
  }

  private static class MyMapFunction implements MapFunction<Profile, KV<Integer, Profile>> {

    private static Map<String, MyMapFunction> taskToMapFunctionMap = new HashMap<>();

    private transient List<Profile> received;
    private transient ReadableTable table;

    @Override
    public void init(Context context) {
      table = (ReadableTable) context.getTaskContext().getTable("t1");
      this.received = new ArrayList<>();

      taskToMapFunctionMap.put(context.getTaskContext().getTaskModel().getTaskName().getTaskName(), this);
    }

    @Override
    public KV<Integer, Profile> apply(Profile profile) {
      received.add(profile);
      return new KV(profile.getMemberId(), profile);
    }

    public static MyMapFunction getMapFunctionByTask(String taskName) {
      return taskToMapFunctionMap.get(taskName);
    }
  }

  @Test
  public void testAsyncOperation() throws Exception {
    KeyValueStore kvStore = mock(KeyValueStore.class);
    LocalStoreBackedReadWriteTable<String, String> table = new LocalStoreBackedReadWriteTable<>("table1", kvStore);
    Context context = mock(Context.class);
    TaskContext taskContext = mock(TaskContext.class);
    when(context.getTaskContext()).thenReturn(taskContext);
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doReturn(mock(Timer.class)).when(metricsRegistry).newTimer(anyString(), anyString());
    doReturn(mock(Counter.class)).when(metricsRegistry).newCounter(anyString(), anyString());
    doReturn(mock(Gauge.class)).when(metricsRegistry).newGauge(anyString(), any());
    doReturn(metricsRegistry).when(taskContext).getTaskMetricsRegistry();

    table.init(context);

    // GET
    doReturn("bar").when(kvStore).get(anyString());
    Assert.assertEquals("bar", table.getAsync("foo").get());

    // GET-ALL
    Map<String, String> recordMap = new HashMap<>();
    recordMap.put("foo1", "bar1");
    recordMap.put("foo2", "bar2");
    doReturn(recordMap).when(kvStore).getAll(anyList());
    Assert.assertEquals(recordMap, table.getAllAsync(Arrays.asList("foo1", "foo2")).get());

    // PUT
    table.putAsync("foo1", "bar1").get();
    verify(kvStore, times(1)).put(anyString(), anyString());

    // PUT-ALL
    List<Entry<String, String>> records = Arrays.asList(new Entry<>("foo1", "bar1"), new Entry<>("foo2", "bar2"));
    table.putAllAsync(records).get();
    verify(kvStore, times(1)).putAll(anyList());

    // DELETE
    table.deleteAsync("foo").get();
    verify(kvStore, times(1)).delete(anyString());

    // DELETE-ALL
    table.deleteAllAsync(Arrays.asList("foo1", "foo2")).get();
    verify(kvStore, times(1)).deleteAll(anyList());
  }

  @Test
  public void testWithLowLevelApi() throws Exception {

    Map<String, String> configs = getBaseJobConfig(bootstrapUrl(), zkConnect());
    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(TestTableData.generatePageViews(10)));
    configs.put("streams.PageView.partitionCount", String.valueOf(4));
    configs.put("task.inputs", "test.PageView");

    final LocalApplicationRunner runner = new LocalApplicationRunner(new MyTaskApplication(), new MapConfig(configs));
    runner.run();
    runner.waitForFinish();
  }

  static public class MyTaskApplication implements TaskApplication {
    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
      appDescriptor.setTaskFactory((StreamTaskFactory) () -> new MyStreamTask());
      appDescriptor.addTable(new InMemoryTableDescriptor("t1", KVSerde.of(new IntegerSerde(), new PageViewJsonSerde())));
      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      GenericInputDescriptor<PageView> pageViewISD = ksd.getInputDescriptor("PageView", new NoOpSerde<>());
      appDescriptor.addInputStream(pageViewISD);
    }
  }

  static public class MyStreamTask implements StreamTask, InitableTask {
    private ReadWriteTable<Integer, PageView> pageViewTable;
    @Override
    public void init(Context context) throws Exception {
      pageViewTable = (ReadWriteTable<Integer, PageView>) context.getTaskContext().getTable("t1");
    }
    @Override
    public void process(IncomingMessageEnvelope message, MessageCollector collector, TaskCoordinator coordinator) {
      PageView pv = (PageView) message.getMessage();
      pageViewTable.put(pv.getMemberId(), pv);
      PageView pv2 = pageViewTable.get(pv.getMemberId());
      Assert.assertEquals(pv.getMemberId(), pv2.getMemberId());
      Assert.assertEquals(pv.getPageKey(), pv2.getPageKey());
    }
  }
}
