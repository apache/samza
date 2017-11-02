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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.RecordTable;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.storage.kv.inmemory.InMemoryTableDescriptor;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.ArraySystemFactory;
import org.apache.samza.test.util.Base64Serializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * This test class tests writeTo() and join() for local tables
 */
public class TestLocalTable extends AbstractIntegrationTestHarness {

  @Test
  public void testWriteTo() throws  Exception {

    List<TestTableData.Profile> received = new ArrayList<>();

    int count = 10;
    TestTableData.Profile[] profiles = TestTableData.generateProfiles(count);

    int partitionCount = 4;
    Map<String, String> configs = getBaseJobConfig();

    configs.put("streams.Profile.samza.system", "test");
    configs.put("streams.Profile.source", Base64Serializer.serialize(profiles));
    configs.put("streams.Profile.partitionCount", String.valueOf(partitionCount));

    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    final StreamApplication app = (streamGraph, cfg) -> {

      RecordTable<Integer, TestTableData.Profile> table = streamGraph.getRecordTable(
          new InMemoryTableDescriptor.Factory<Integer, TestTableData.Profile>().getTableDescriptor("t1")
              .withKeySerde(new IntegerSerde())
              .withValueSerde(new TestTableData.ProfileJsonSerde()));

      streamGraph.getInputStream("Profile", new NoOpSerde<TestTableData.Profile>())
          .writeTo(table, m -> m.getMemberId(), m -> {
              received.add(m);
              return m;
            });
    };

    runner.run(app);
    runner.waitForFinish();

    assertEquals(count * partitionCount, received.size());
    assertEquals(count, new HashSet(received).size());
  }

  @Test
  public void testStreamTableJoin() throws  Exception {

    List<TestTableData.PageView> received = new ArrayList<>();

    int count = 10;
    TestTableData.PageView[] pageViews = TestTableData.generatePageViews(count);
    TestTableData.Profile[] profiles = TestTableData.generateProfiles(count);

    int partitionCount = 4;
    Map<String, String> configs = getBaseJobConfig();

    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    configs.put("streams.Profile.samza.system", "test");
    configs.put("streams.Profile.samza.bootstrap", "true");
    configs.put("streams.Profile.source", Base64Serializer.serialize(profiles));
    configs.put("streams.Profile.partitionCount", String.valueOf(partitionCount));

    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    final StreamApplication app = (streamGraph, cfg) -> {

      RecordTable<Integer, TestTableData.Profile> table = streamGraph.getRecordTable(
          new InMemoryTableDescriptor.Factory<Integer, TestTableData.Profile>().getTableDescriptor("t1")
              .withKeySerde(new IntegerSerde())
              .withValueSerde(new TestTableData.ProfileJsonSerde()));

      streamGraph.getInputStream("Profile", new NoOpSerde<TestTableData.Profile>())
          .writeTo(table, m -> m.getMemberId(), m -> m);

      streamGraph.getInputStream("PageView", new NoOpSerde<TestTableData.PageView>())
          .partitionBy(TestTableData.PageView::getMemberId, v -> v, "p1")
          .join(table, new MyJoinFn())
          .sink((m, collector, coordinator) -> received.add(m));
    };

    runner.run(app);
    runner.waitForFinish();

    assertEquals(count * partitionCount, received.size());
    assertTrue(received.get(0) instanceof TestTableData.EnrichedPageView);

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
    configs.put("serializers.registry.json.class", TestTableData.PageViewJsonSerdeFactory.class.getName());

    return configs;
  }

  static public class MyJoinFn implements StreamTableJoinFunction
      <Integer, KV<Integer, TestTableData.PageView>, TestTableData.Profile, TestTableData.EnrichedPageView> {
    @Override
    public TestTableData.EnrichedPageView apply(KV<Integer, TestTableData.PageView> kv, TestTableData.Profile p) {
      TestTableData.PageView pv = kv.getValue();
      return new TestTableData.EnrichedPageView(pv.getPageKey(), pv.getMemberId(), p.getCompany());
    }

    @Override
    public Integer getFirstKey(KV<Integer, TestTableData.PageView> kv) {
      return kv.getValue().getMemberId();
    }
  }

}
