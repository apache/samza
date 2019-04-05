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
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.context.Context;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.Table;
import org.apache.samza.test.harness.IntegrationTestHarness;
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


/**
 * This test class tests sendTo() and join() for local tables
 */
public class TestLocalTableEndToEnd extends IntegrationTestHarness {

  @Test
  public void testSendTo() throws Exception {

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

    Config config = new MapConfig(configs);
    final LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    executeRun(runner, config);
    runner.waitForFinish();

    for (int i = 0; i < partitionCount; i++) {
      MyMapFunction mapFnCopy = MyMapFunction.getMapFunctionByTask(String.format("Partition %d", i));
      assertEquals(count, mapFnCopy.received.size());
      mapFnCopy.received.forEach(p -> Assert.assertTrue(mapFnCopy.table.get(p.getMemberId()) != null));
    }
  }

  static class StreamTableJoinApp implements StreamApplication {
    static List<PageView> received = new LinkedList<>();
    static List<EnrichedPageView> joined = new LinkedList<>();

    @Override
    public void describe(StreamApplicationDescriptor appDesc) {
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

    Config config = new MapConfig(configs);
    final LocalApplicationRunner runner = new LocalApplicationRunner(new StreamTableJoinApp(), config);
    executeRun(runner, config);
    runner.waitForFinish();

    assertEquals(count * partitionCount, StreamTableJoinApp.received.size());
    assertEquals(count * partitionCount, StreamTableJoinApp.joined.size());
    assertTrue(StreamTableJoinApp.joined.get(0) instanceof EnrichedPageView);
  }

  static class DualStreamTableJoinApp implements StreamApplication {
    static List<Profile> sentToProfileTable1 = new LinkedList<>();
    static List<Profile> sentToProfileTable2 = new LinkedList<>();
    static List<EnrichedPageView> joinedPageViews1 = new LinkedList<>();
    static List<EnrichedPageView> joinedPageViews2 = new LinkedList<>();

    @Override
    public void describe(StreamApplicationDescriptor appDesc) {
      KVSerde<Integer, Profile> profileKVSerde = KVSerde.of(new IntegerSerde(), new ProfileJsonSerde());
      KVSerde<Integer, PageView> pageViewKVSerde = KVSerde.of(new IntegerSerde(), new PageViewJsonSerde());

      PageViewToProfileJoinFunction joinFn1 = new PageViewToProfileJoinFunction();
      PageViewToProfileJoinFunction joinFn2 = new PageViewToProfileJoinFunction();

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

    Config config = new MapConfig(configs);
    final LocalApplicationRunner runner = new LocalApplicationRunner(new DualStreamTableJoinApp(), config);
    executeRun(runner, config);
    runner.waitForFinish();

    assertEquals(count * partitionCount, DualStreamTableJoinApp.sentToProfileTable1.size());
    assertEquals(count * partitionCount, DualStreamTableJoinApp.sentToProfileTable2.size());

    assertEquals(count * partitionCount, DualStreamTableJoinApp.joinedPageViews1.size());
    assertEquals(count * partitionCount, DualStreamTableJoinApp.joinedPageViews2.size());
    assertTrue(DualStreamTableJoinApp.joinedPageViews1.get(0) instanceof EnrichedPageView);
    assertTrue(DualStreamTableJoinApp.joinedPageViews2.get(0) instanceof EnrichedPageView);
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
    private transient ReadWriteTable table;

    @Override
    public void init(Context context) {
      table = context.getTaskContext().getTable("t1");
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

}
