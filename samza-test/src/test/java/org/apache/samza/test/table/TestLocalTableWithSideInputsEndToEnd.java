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
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.harness.IntegrationTestHarness;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.EnrichedPageView;
import static org.apache.samza.test.table.TestTableData.PageView;
import static org.apache.samza.test.table.TestTableData.Profile;
import static org.apache.samza.test.table.TestTableData.ProfileJsonSerde;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestLocalTableWithSideInputsEndToEnd extends IntegrationTestHarness {
  private static final String PAGEVIEW_STREAM = "pageview";
  private static final String PROFILE_STREAM = "profile";
  private static final String ENRICHED_PAGEVIEW_STREAM = "enrichedpageview";

  @Test
  public void testJoinWithSideInputsTable() {
    runTest(
        "test",
        new PageViewProfileJoin(),
        Arrays.asList(TestTableData.generatePageViews(10)),
        Arrays.asList(TestTableData.generateProfiles(10)));
  }

  @Test
  public void testJoinWithDurableSideInputTable() {
    runTest(
        "test",
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

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(systemName);

    InMemoryInputDescriptor<PageView> pageViewStreamDesc = isd
        .getInputDescriptor(PAGEVIEW_STREAM, new NoOpSerde<PageView>());

    InMemoryInputDescriptor<Profile> profileStreamDesc = isd
        .getInputDescriptor(PROFILE_STREAM, new NoOpSerde<Profile>());

    InMemoryOutputDescriptor<EnrichedPageView> outputStreamDesc = isd
        .getOutputDescriptor(ENRICHED_PAGEVIEW_STREAM, new NoOpSerde<EnrichedPageView>());

    TestRunner
        .of(app)
        .addInputStream(pageViewStreamDesc, pageViews)
        .addInputStream(profileStreamDesc, profiles)
        .addOutputStream(outputStreamDesc, 1)
        .addConfig(new MapConfig(configs))
        .run(Duration.ofMillis(100000));

    try {
      Map<Integer, List<EnrichedPageView>> result = TestRunner.consumeStream(outputStreamDesc, Duration.ofMillis(1000));
      List<EnrichedPageView> results = result.values().stream()
          .flatMap(List::stream)
          .collect(Collectors.toList());

      List<EnrichedPageView> expectedEnrichedPageviews = pageViews.stream()
          .flatMap(pv -> profiles.stream()
              .filter(profile -> pv.memberId == profile.memberId)
              .map(profile -> new EnrichedPageView(pv.pageKey, profile.memberId, profile.company)))
          .collect(Collectors.toList());

      boolean successfulJoin = results.stream().allMatch(expectedEnrichedPageviews::contains);
      assertEquals("Mismatch between the expected and actual join count", expectedEnrichedPageviews.size(), results.size());
      assertTrue("Pageview profile join did not succeed for all inputs", successfulJoin);
    } catch (SamzaException e) {
      e.printStackTrace();
    }
  }

  static class PageViewProfileJoin implements StreamApplication {
    static final String PROFILE_TABLE = "profile-table";

    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      Table<KV<Integer, TestTableData.Profile>> table = appDescriptor.getTable(getTableDescriptor());
      KafkaSystemDescriptor sd =
          new KafkaSystemDescriptor("test");
      appDescriptor.getInputStream(sd.getInputDescriptor(PAGEVIEW_STREAM, new NoOpSerde<TestTableData.PageView>()))
          .partitionBy(TestTableData.PageView::getMemberId, v -> v, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()), "partition-page-view")
          .join(table, new PageViewToProfileJoinFunction())
          .sendTo(appDescriptor.getOutputStream(sd.getOutputDescriptor(ENRICHED_PAGEVIEW_STREAM, new NoOpSerde<>())));
    }

    protected TableDescriptor<Integer, Profile, ?> getTableDescriptor() {
      return new InMemoryTableDescriptor(PROFILE_TABLE, KVSerde.of(new IntegerSerde(), new ProfileJsonSerde()))
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
      return new RocksDbTableDescriptor(PROFILE_TABLE, KVSerde.of(new IntegerSerde(), new ProfileJsonSerde()))
          .withSideInputs(ImmutableList.of(PROFILE_STREAM))
          .withSideInputsProcessor((msg, store) -> {
              TestTableData.Profile profile = (TestTableData.Profile) msg.getMessage();
              int key = profile.getMemberId();
              return ImmutableList.of(new Entry<>(key, profile));
            });
    }
  }
}