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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.storage.kv.inmemory.InMemoryTableDescriptor;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.task.TaskContext;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.table.TestTableData.EnrichedPageView;
import org.apache.samza.test.table.TestTableData.PageView;
import org.apache.samza.test.table.TestTableData.PageViewJsonSerde;
import org.apache.samza.test.table.TestTableData.PageViewJsonSerdeFactory;
import org.apache.samza.test.table.TestTableData.Profile;
import org.apache.samza.test.table.TestTableData.ProfileJsonSerde;
import org.apache.samza.test.util.ArraySystemFactory;
import org.apache.samza.test.util.Base64Serializer;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test class tests sendTo() and join() for local tables
 */
public class TestLocalTable extends AbstractIntegrationTestHarness {

  @Test
  public void testSendTo() throws  Exception {

    int count = 10;
    Profile[] profiles = TestTableData.generateProfiles(count);

    int partitionCount = 4;
    Map<String, String> configs = getBaseJobConfig();

    configs.put("streams.Profile.samza.system", "test");
    configs.put("streams.Profile.source", Base64Serializer.serialize(profiles));
    configs.put("streams.Profile.partitionCount", String.valueOf(partitionCount));

    MyMapFunction mapFn = new MyMapFunction();

    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    final StreamApplication app = (streamGraph, cfg) -> {

      Table<KV<Integer, Profile>> table = streamGraph.getTable(new InMemoryTableDescriptor("t1")
          .withSerde(KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())));

      streamGraph.getInputStream("Profile", new NoOpSerde<Profile>())
          .map(mapFn)
          .sendTo(table);
    };

    runner.run(app);
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
      final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
      final StreamApplication app = (streamGraph, cfg) -> {

        Table<KV<Integer, Profile>> table = streamGraph.getTable(
            new InMemoryTableDescriptor("t1").withSerde(KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())));

        streamGraph.getInputStream("Profile", new NoOpSerde<Profile>())
            .map(m -> new KV(m.getMemberId(), m))
            .sendTo(table);

        streamGraph.getInputStream("PageView", new NoOpSerde<PageView>())
            .map(pv -> {
                received.add(pv);
                return pv;
              })
            .partitionBy(PageView::getMemberId, v -> v, "p1")
            .join(table, new PageViewToProfileJoinFunction())
            .sink((m, collector, coordinator) -> joined.add(m));
      };

      runner.run(app);
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
    Map<String, String> configs = getBaseJobConfig();

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

      final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
      final StreamApplication app = (streamGraph, cfg) -> {

        Table<KV<Integer, Profile>> profileTable = streamGraph.getTable(new InMemoryTableDescriptor("t1")
            .withSerde(profileKVSerde));

        MessageStream<Profile> profileStream1 = streamGraph.getInputStream("Profile1", new NoOpSerde<Profile>());
        MessageStream<Profile> profileStream2 = streamGraph.getInputStream("Profile2", new NoOpSerde<Profile>());

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

        MessageStream<PageView> pageViewStream1 = streamGraph.getInputStream("PageView1", new NoOpSerde<PageView>());
        MessageStream<PageView> pageViewStream2 = streamGraph.getInputStream("PageView2", new NoOpSerde<PageView>());

        pageViewStream1
            .partitionBy(PageView::getMemberId, v -> v, pageViewKVSerde, "p1")
            .join(profileTable, joinFn1)
            .sink((m, collector, coordinator) -> joinedPageViews1.add(m));

        pageViewStream2
            .partitionBy(PageView::getMemberId, v -> v, pageViewKVSerde, "p2")
            .join(profileTable, joinFn2)
            .sink((m, collector, coordinator) -> joinedPageViews2.add(m));
      };

      runner.run(app);
      runner.waitForFinish();

      assertEquals(count * partitionCount, sentToProfileTable1.size());
      assertEquals(count * partitionCount, sentToProfileTable2.size());
      assertEquals(count * partitionCount, joinFn1.count);
      assertEquals(count * partitionCount, joinFn2.count);
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
    Map<String, String> configs = getBaseJobConfig();

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

  private Map<String, String> getBaseJobConfig() {
    Map<String, String> configs = new HashMap<>();
    configs.put("systems.test.samza.factory", ArraySystemFactory.class.getName());

    configs.put(JobConfig.JOB_NAME(), "test-table-job");
    configs.put(JobConfig.PROCESSOR_ID(), "1");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    // For intermediate streams
    configs.put("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems.kafka.producer.bootstrap.servers", bootstrapUrl());
    configs.put("systems.kafka.consumer.zookeeper.connect", zkConnect());
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
    public void init(Config config, TaskContext context) {
      table = (ReadableTable) context.getTable("t1");
      this.received = new ArrayList<>();

      taskToMapFunctionMap.put(context.getTaskName().getTaskName(), this);
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

  private static class PageViewToProfileJoinFunction implements StreamTableJoinFunction
      <Integer, KV<Integer, PageView>, KV<Integer, Profile>, EnrichedPageView> {
    private int count;
    @Override
    public EnrichedPageView apply(KV<Integer, PageView> m, KV<Integer, Profile> r) {
      ++count;
      return r == null ? null :
          new EnrichedPageView(m.getValue().getPageKey(), m.getKey(), r.getValue().getCompany());
    }

    @Override
    public Integer getMessageKey(KV<Integer, PageView> message) {
      return message.getKey();
    }

    @Override
    public Integer getRecordKey(KV<Integer, Profile> record) {
      return record.getKey();
    }
  }
}
