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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.context.Context;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.Table;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.util.ArraySystemFactory;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.EnrichedPageView;
import static org.apache.samza.test.table.TestTableData.PageView;
import static org.apache.samza.test.table.TestTableData.PageViewJsonSerde;
import static org.apache.samza.test.table.TestTableData.PageViewJsonSerdeFactory;
import static org.apache.samza.test.table.TestTableData.Profile;
import static org.apache.samza.test.table.TestTableData.ProfileJsonSerde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * This test class tests sendTo() and join() for local tables
 */
public class TestLocalTableEndToEnd {
  private static final String SYSTEM_NAME = "test";
  private static final String PAGEVIEW_STREAM = "pageview";
  private static final String PROFILE_STREAM = "profile";

  @Test
  public void testSendTo() {
    MyMapFunction mapFn = new MyMapFunction();
    StreamApplication app = appDesc -> {
      Table<KV<Integer, Profile>> table =
          appDesc.getTable(new InMemoryTableDescriptor<>("t1", KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())));
      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor(SYSTEM_NAME);
      GenericInputDescriptor<Profile> isd = ksd.getInputDescriptor(PROFILE_STREAM, new NoOpSerde<>());

      appDesc.getInputStream(isd).map(mapFn).sendTo(table);
    };

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME);
    InMemoryInputDescriptor<Profile> profileStreamDesc = isd.getInputDescriptor(PROFILE_STREAM, new NoOpSerde<>());

    int numProfilesPerPartition = 10;
    int numInputPartitions = 4;
    Map<Integer, List<Profile>> inputProfiles =
        TestTableData.generatePartitionedProfiles(numProfilesPerPartition * numInputPartitions, numInputPartitions);
    TestRunner.of(app).addInputStream(profileStreamDesc, inputProfiles).run(Duration.ofSeconds(10));

    for (int i = 0; i < numInputPartitions; i++) {
      MyMapFunction mapFnCopy = MyMapFunction.getMapFunctionByTask(String.format("Partition %d", i));
      assertEquals(numProfilesPerPartition, mapFnCopy.received.size());
      mapFnCopy.received.forEach(p -> assertNotNull(mapFnCopy.table.get(p.getMemberId())));
    }
  }

  static class StreamTableJoinApp implements StreamApplication {
    static List<PageView> received = new LinkedList<>();
    static List<EnrichedPageView> joined = new LinkedList<>();

    @Override
    public void describe(StreamApplicationDescriptor appDesc) {
      Table<KV<Integer, Profile>> table = appDesc.getTable(
          new InMemoryTableDescriptor<>("t1", KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())));
      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor(SYSTEM_NAME);
      GenericInputDescriptor<Profile> profileISD = ksd.getInputDescriptor(PROFILE_STREAM, new NoOpSerde<>());
      profileISD.shouldBootstrap();
      appDesc.getInputStream(profileISD)
          .map(m -> new KV<>(m.getMemberId(), m))
          .sendTo(table);

      GenericInputDescriptor<PageView> pageViewISD = ksd.getInputDescriptor(PAGEVIEW_STREAM, new NoOpSerde<>());
      appDesc.getInputStream(pageViewISD)
          .map(pv -> {
            received.add(pv);
            return pv;
          })
          .partitionBy(PageView::getMemberId, v -> v, KVSerde.of(new IntegerSerde(), new PageViewJsonSerde()), "p1")
          .join(table, new PageViewToProfileJoinFunction())
          .sink((m, collector, coordinator) -> joined.add(m));
    }
  }

  @Test
  public void testStreamTableJoin() {
    int totalPageViews = 40;
    int partitionCount = 4;
    Map<Integer, List<PageView>> inputPageViews =
        TestTableData.generatePartitionedPageViews(totalPageViews, partitionCount);
    // 10 is the max member id for page views
    Map<Integer, List<Profile>> inputProfiles =
        TestTableData.generatePartitionedProfiles(10, partitionCount);
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME);
    InMemoryInputDescriptor<PageView> pageViewStreamDesc = isd
        .getInputDescriptor(PAGEVIEW_STREAM, new NoOpSerde<>());
    InMemoryInputDescriptor<Profile> profileStreamDesc = isd
        .getInputDescriptor(PROFILE_STREAM, new NoOpSerde<>());

    TestRunner.of(new StreamTableJoinApp())
        .addInputStream(pageViewStreamDesc, inputPageViews)
        .addInputStream(profileStreamDesc, inputProfiles)
        .run(Duration.ofSeconds(10));

    assertEquals(totalPageViews, StreamTableJoinApp.received.size());
    assertEquals(totalPageViews, StreamTableJoinApp.joined.size());
    assertNotNull(StreamTableJoinApp.joined.get(0));
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

      Table<KV<Integer, Profile>> profileTable = appDesc.getTable(new InMemoryTableDescriptor<>("t1", profileKVSerde));

      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor(SYSTEM_NAME);
      GenericInputDescriptor<Profile> profileISD1 = ksd.getInputDescriptor(PROFILE_STREAM + "1", new NoOpSerde<>());
      profileISD1.shouldBootstrap();
      GenericInputDescriptor<Profile> profileISD2 = ksd.getInputDescriptor(PROFILE_STREAM + "2", new NoOpSerde<>());
      profileISD2.shouldBootstrap();
      MessageStream<Profile> profileStream1 = appDesc.getInputStream(profileISD1);
      MessageStream<Profile> profileStream2 = appDesc.getInputStream(profileISD2);

      profileStream1
          .map(m -> {
            sentToProfileTable1.add(m);
            return new KV<>(m.getMemberId(), m);
          })
          .sendTo(profileTable);
      profileStream2
          .map(m -> {
            sentToProfileTable2.add(m);
            return new KV<>(m.getMemberId(), m);
          })
          .sendTo(profileTable);

      GenericInputDescriptor<PageView> pageViewISD1 = ksd.getInputDescriptor(PAGEVIEW_STREAM + "1", new NoOpSerde<>());
      GenericInputDescriptor<PageView> pageViewISD2 = ksd.getInputDescriptor(PAGEVIEW_STREAM + "2", new NoOpSerde<>());
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
  public void testDualStreamTableJoin() {
    int totalPageViews = 40;
    int partitionCount = 4;
    Map<Integer, List<PageView>> inputPageViews1 =
        TestTableData.generatePartitionedPageViews(totalPageViews, partitionCount);
    Map<Integer, List<PageView>> inputPageViews2 =
        TestTableData.generatePartitionedPageViews(totalPageViews, partitionCount);
    // 10 is the max member id for page views
    int numProfiles = 10;
    Map<Integer, List<Profile>> inputProfiles1 = TestTableData.generatePartitionedProfiles(numProfiles, partitionCount);
    Map<Integer, List<Profile>> inputProfiles2 = TestTableData.generatePartitionedProfiles(numProfiles, partitionCount);
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME);
    InMemoryInputDescriptor<PageView> pageViewStreamDesc1 = isd
        .getInputDescriptor(PAGEVIEW_STREAM + "1", new NoOpSerde<>());
    InMemoryInputDescriptor<PageView> pageViewStreamDesc2 = isd
        .getInputDescriptor(PAGEVIEW_STREAM + "2", new NoOpSerde<>());
    InMemoryInputDescriptor<Profile> profileStreamDesc1 = isd
        .getInputDescriptor(PROFILE_STREAM + "1", new NoOpSerde<>());
    InMemoryInputDescriptor<Profile> profileStreamDesc2 = isd
        .getInputDescriptor(PROFILE_STREAM + "2", new NoOpSerde<>());

    TestRunner.of(new DualStreamTableJoinApp())
        .addInputStream(pageViewStreamDesc1, inputPageViews1)
        .addInputStream(pageViewStreamDesc2, inputPageViews2)
        .addInputStream(profileStreamDesc1, inputProfiles1)
        .addInputStream(profileStreamDesc2, inputProfiles2)
        .run(Duration.ofSeconds(10));

    assertEquals(numProfiles, DualStreamTableJoinApp.sentToProfileTable1.size());
    assertEquals(numProfiles, DualStreamTableJoinApp.sentToProfileTable2.size());
    assertEquals(totalPageViews, DualStreamTableJoinApp.joinedPageViews1.size());
    assertEquals(totalPageViews, DualStreamTableJoinApp.joinedPageViews2.size());
    assertNotNull(DualStreamTableJoinApp.joinedPageViews1.get(0));
    assertNotNull(DualStreamTableJoinApp.joinedPageViews2.get(0));
  }

  static Map<String, String> getBaseJobConfig(String bootstrapUrl, String zkConnect) {
    Map<String, String> configs = new HashMap<>();
    configs.put("systems.test.samza.factory", ArraySystemFactory.class.getName());

    configs.put(JobConfig.JOB_NAME, "test-table-job");
    configs.put(JobConfig.PROCESSOR_ID, "1");
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.put(TaskConfig.GROUPER_FACTORY, SingleContainerGrouperFactory.class.getName());

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
    private static final Map<String, MyMapFunction> TASK_TO_MAP_FUNCTION_MAP = new HashMap<>();

    private transient List<Profile> received;
    private transient ReadWriteTable table;

    @Override
    public void init(Context context) {
      table = context.getTaskContext().getTable("t1");
      this.received = new ArrayList<>();

      TASK_TO_MAP_FUNCTION_MAP.put(context.getTaskContext().getTaskModel().getTaskName().getTaskName(), this);
    }

    @Override
    public KV<Integer, Profile> apply(Profile profile) {
      received.add(profile);
      return new KV<>(profile.getMemberId(), profile);
    }

    public static MyMapFunction getMapFunctionByTask(String taskName) {
      return TASK_TO_MAP_FUNCTION_MAP.get(taskName);
    }
  }

}
