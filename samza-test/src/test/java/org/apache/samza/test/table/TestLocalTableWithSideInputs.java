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

import com.google.common.collect.ImmutableList;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.storage.kv.inmemory.InMemoryTableDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestLocalTableWithSideInputs extends AbstractIntegrationTestHarness {
  private static final String PAGEVIEW_STREAM = "pageview";
  private static final String PROFILE_STREAM = "profile";
  private static final String ENRICHED_PAGEVIEW_STREAM = "enrichedpageview";

  @Test
  public void testJoinWithSideInputsTable() {
    runTest(
        "side-input-join",
        new PageViewProfileJoin(),
        Arrays.asList(TestTableData.generatePageViews(10)),
        Arrays.asList(TestTableData.generateProfiles(10)));
  }

  @Test
  public void testJoinWithDurableSideInputTable() {
    runTest(
        "durable-side-input",
        new DurablePageViewProfileJoin(),
        Arrays.asList(TestTableData.generatePageViews(5)),
        Arrays.asList(TestTableData.generateProfiles(5)));
  }

  private void runTest(String systemName, StreamApplication app, List<PageView> pageViews,
      List<Profile> profiles) {
    Map<String, String> configs = new HashMap<>();
    configs.put(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), PAGEVIEW_STREAM), systemName);
    configs.put(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), PROFILE_STREAM), systemName);
    configs.put(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), ENRICHED_PAGEVIEW_STREAM), systemName);
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), systemName);

    CollectionStream<PageView> pageViewStream =
        CollectionStream.of(systemName, PAGEVIEW_STREAM, pageViews);
    CollectionStream<Profile> profileStream =
        CollectionStream.of(systemName, PROFILE_STREAM, profiles);

    CollectionStream<EnrichedPageView> outputStream =
        CollectionStream.empty(systemName, ENRICHED_PAGEVIEW_STREAM);

    TestRunner
        .of(app)
        .addInputStream(pageViewStream)
        .addInputStream(profileStream)
        .addOutputStream(outputStream)
        .addConfigs(new MapConfig(configs))
        .addOverrideConfig(ClusterManagerConfig.CLUSTER_MANAGER_HOST_AFFINITY_ENABLED, Boolean.FALSE.toString())
        .run(Duration.ofMillis(100000));

    try {
      Map<Integer, List<EnrichedPageView>> result = TestRunner.consumeStream(outputStream, Duration.ofMillis(1000));
      List<EnrichedPageView> results = result.values().stream()
          .flatMap(List::stream)
          .collect(Collectors.toList());

      List<EnrichedPageView> expectedEnrichedPageviews = pageViews.stream()
          .flatMap(pv -> profiles.stream()
              .filter(profile -> pv.memberId == profile.memberId)
              .map(profile -> new EnrichedPageView(pv.pageKey, profile.memberId, profile.company)))
          .collect(Collectors.toList());

      boolean successfulJoin = results.stream().allMatch(expectedEnrichedPageviews::contains);
      assertEquals("Mismatch between the expected and actual join count", results.size(),
          expectedEnrichedPageviews.size());
      assertTrue("Pageview profile join did not succeed for all inputs", successfulJoin);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  static class PageViewProfileJoin implements StreamApplication {
    static final String PROFILE_TABLE = "profile-table";

    @Override
    public void describe(StreamApplicationDescriptor appDesc) {
      Table<KV<Integer, TestTableData.Profile>> table = appDesc.getTable(getTableDescriptor());
      KafkaSystemDescriptor sd =
          new KafkaSystemDescriptor(appDesc.getConfig().get(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), PAGEVIEW_STREAM)));
      appDesc.getInputStream(sd.getInputDescriptor(PAGEVIEW_STREAM, new NoOpSerde<TestTableData.PageView>()))
          .partitionBy(TestTableData.PageView::getMemberId, v -> v, "partition-page-view")
          .join(table, new PageViewToProfileJoinFunction())
          .sendTo(appDesc.getOutputStream(sd.getOutputDescriptor(ENRICHED_PAGEVIEW_STREAM, new NoOpSerde<>())));
    }

    protected TableDescriptor<Integer, Profile, ?> getTableDescriptor() {
      return new InMemoryTableDescriptor<Integer, Profile>(PROFILE_TABLE)
          .withSerde(KVSerde.of(new IntegerSerde(), new ProfileJsonSerde()))
          .withSideInputs(ImmutableList.of(PROFILE_STREAM))
          .withSideInputsProcessor((msg, store) -> {
              Profile profile = (Profile) msg.getMessage();
              int key = profile.getMemberId();

              return ImmutableList.of(new Entry<>(key, profile));
            });
    }
  }

  static class DurablePageViewProfileJoin extends PageViewProfileJoin {
    @Override
    protected TableDescriptor<Integer, Profile, ?> getTableDescriptor() {
      return new RocksDbTableDescriptor<Integer, Profile>(PROFILE_TABLE)
          .withSerde(KVSerde.of(new IntegerSerde(), new ProfileJsonSerde()))
          .withSideInputs(ImmutableList.of(PROFILE_STREAM))
          .withSideInputsProcessor((msg, store) -> {
              TestTableData.Profile profile = (TestTableData.Profile) msg.getMessage();
              int key = profile.getMemberId();

              return ImmutableList.of(new Entry<>(key, profile));
            });
    }
  }
}
