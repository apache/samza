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
import org.apache.samza.operators.functions.KeyedStreamTableJoinFunction;
import org.apache.samza.operators.functions.MapFunction;
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

    assertEquals(count * partitionCount, mapFn.received.size());
    assertEquals(count, new HashSet(mapFn.received).size());
    mapFn.received.forEach(p -> Assert.assertTrue(mapFn.table.get(p.getMemberId()) != null));
  }

  @Test
  public void testStreamTableJoin() throws Exception {

    List<PageView> received = new LinkedList<>();
    List<EnrichedPageView> joined = new LinkedList<>();

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

    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));

    final StreamApplication app = (streamGraph, cfg) -> {

      Table<KV<Integer, Profile>> table = streamGraph.getTable(new InMemoryTableDescriptor("t1")
          .withSerde(KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())));

      streamGraph.getInputStream("Profile", new NoOpSerde<Profile>())
          .map(m -> new KV(m.getMemberId(), m))
          .sendTo(table);

      streamGraph.getInputStream("PageView", new NoOpSerde<PageView>())
          .map(pv -> {
              received.add(pv);
              return pv;
            })
          .partitionBy(PageView::getMemberId, v -> v, "p1")
          .join(table, (KeyedStreamTableJoinFunction<Integer, PageView, Profile, EnrichedPageView>) (m, r) ->
              new EnrichedPageView(m.getValue().getPageKey(), m.getKey(), r.getValue().getCompany()))
          .sink((m, collector, coordinator) -> joined.add(m));
    };

    runner.run(app);
    runner.waitForFinish();

    assertEquals(count * partitionCount, joined.size());
    assertTrue(joined.get(0) instanceof EnrichedPageView);
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

  private class MyMapFunction implements MapFunction<Profile, KV<Integer, Profile>> {

    private List<Profile> received = new ArrayList<>();
    private ReadableTable table;

    @Override
    public void init(Config config, TaskContext context) {
      table = (ReadableTable) context.getTable("t1");
    }

    @Override
    public KV<Integer, Profile> apply(Profile profile) {
      received.add(profile);
      return new KV(profile.getMemberId(), profile);
    }
  }
}
