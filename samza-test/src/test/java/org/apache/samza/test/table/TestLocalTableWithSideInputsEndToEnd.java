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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.StreamAssert;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.EnrichedPageView;
import static org.apache.samza.test.table.TestTableData.PageView;
import static org.apache.samza.test.table.TestTableData.Profile;
import static org.apache.samza.test.table.TestTableData.ProfileJsonSerde;


public class TestLocalTableWithSideInputsEndToEnd {
  private static final String SYSTEM_NAME = "test";
  private static final String PAGEVIEW_STREAM = "pageview";
  private static final String PROFILE_STREAM = "profile";
  private static final String PROFILE_TABLE = "profile-table";
  private static final String ENRICHED_PAGEVIEW_STREAM = "enrichedpageview";
  private static final SystemStream OUTPUT_SYSTEM_STREAM = new SystemStream(SYSTEM_NAME, ENRICHED_PAGEVIEW_STREAM);

  @Test
  public void testLowLevelJoinWithSideInputsTable() throws InterruptedException {
    int partitionCount = 4;
    IntegerSerde integerSerde = new IntegerSerde();
    // for low-level, need to pre-partition the input in the same way that the profiles are partitioned
    Map<Integer, List<PageView>> pageViewsPartitionedByMemberId =
        TestTableData.generatePartitionedPageViews(20, partitionCount)
            .values()
            .stream()
            .flatMap(List::stream)
            .collect(Collectors.groupingBy(
              pageView -> Math.abs(Arrays.hashCode(integerSerde.toBytes(pageView.getMemberId()))) % partitionCount));
    runTest(
        new LowLevelPageViewProfileJoin(),
        pageViewsPartitionedByMemberId,
        TestTableData.generatePartitionedProfiles(10, partitionCount));
  }

  @Test
  public void testJoinWithSideInputsTable() throws InterruptedException {
    runTest(
        new PageViewProfileJoin(),
        TestTableData.generatePartitionedPageViews(20, 4),
        TestTableData.generatePartitionedProfiles(10, 4));
  }

  @Test
  public void testJoinWithDurableSideInputTable() throws InterruptedException {
    runTest(
        new DurablePageViewProfileJoin(),
        TestTableData.generatePartitionedPageViews(20, 4),
        TestTableData.generatePartitionedProfiles(10, 4));
  }

  private <T extends ApplicationDescriptor<?>> void runTest(SamzaApplication<T> app,
      Map<Integer, List<PageView>> pageViews,
      Map<Integer, List<Profile>> profiles) throws InterruptedException {
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(SYSTEM_NAME);
    InMemoryInputDescriptor<PageView> pageViewStreamDesc = isd
        .getInputDescriptor(PAGEVIEW_STREAM, new NoOpSerde<>());
    InMemoryInputDescriptor<Profile> profileStreamDesc = isd
        .getInputDescriptor(PROFILE_STREAM, new NoOpSerde<>());
    InMemoryOutputDescriptor<EnrichedPageView> outputStreamDesc = isd
        .getOutputDescriptor(ENRICHED_PAGEVIEW_STREAM, new NoOpSerde<>());

    TestRunner.of(app)
        .addInputStream(pageViewStreamDesc, pageViews)
        .addInputStream(profileStreamDesc, profiles)
        .addOutputStream(outputStreamDesc, 1)
        .run(Duration.ofSeconds(10));

    List<EnrichedPageView> expectedEnrichedPageViews = buildExpectedEnrichedPageViews(pageViews, profiles);
    StreamAssert.containsInAnyOrder(expectedEnrichedPageViews, outputStreamDesc, Duration.ofSeconds(1));
  }

  private static List<EnrichedPageView> buildExpectedEnrichedPageViews(Map<Integer, List<PageView>> pageViews,
      Map<Integer, List<Profile>> profiles) {
    ImmutableMap.Builder<Integer, Profile> profilesByMemberIdBuilder = new ImmutableMap.Builder<>();
    profiles.values()
        .stream()
        .flatMap(List::stream)
        .forEach(profile -> profilesByMemberIdBuilder.put(profile.getMemberId(), profile));
    Map<Integer, Profile> profilesByMemberId = profilesByMemberIdBuilder.build();
    ImmutableList.Builder<EnrichedPageView> enrichedPageViewsBuilder = new ImmutableList.Builder<>();
    pageViews.values()
        .stream()
        .flatMap(List::stream)
        .forEach(pageView -> Optional.ofNullable(profilesByMemberId.get(pageView.getMemberId()))
            .ifPresent(profile -> enrichedPageViewsBuilder.add(
                new EnrichedPageView(pageView.getPageKey(), profile.getMemberId(), profile.getCompany()))));
    return enrichedPageViewsBuilder.build();
  }

  static class LowLevelPageViewProfileJoin implements TaskApplication {
    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
      DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor(SYSTEM_NAME);
      appDescriptor.withInputStream(sd.getInputDescriptor(PAGEVIEW_STREAM, new NoOpSerde<>()));
      appDescriptor.withInputStream(sd.getInputDescriptor(PROFILE_STREAM, new NoOpSerde<>()));

      TableDescriptor<Integer, Profile, ?> tableDescriptor = new InMemoryTableDescriptor<>(PROFILE_TABLE,
          KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())).withSideInputs(ImmutableList.of(PROFILE_STREAM))
          .withSideInputsProcessor((msg, store) -> {
            Profile profile = (Profile) msg.getMessage();
            int key = profile.getMemberId();
            return ImmutableList.of(new Entry<>(key, profile));
          });
      appDescriptor.withTable(tableDescriptor);

      appDescriptor.withOutputStream(sd.getOutputDescriptor(ENRICHED_PAGEVIEW_STREAM, new NoOpSerde<>()));

      appDescriptor.withTaskFactory((StreamTaskFactory) PageViewProfileJoinStreamTask::new);
    }
  }

  static class PageViewProfileJoinStreamTask implements InitableTask, StreamTask {
    private ReadWriteTable<Integer, Profile> profileTable;

    @Override
    public void init(Context context) {
      this.profileTable = context.getTaskContext().getTable(PROFILE_TABLE);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
      PageView pageView = (PageView) envelope.getMessage();
      Profile profile = this.profileTable.get(pageView.getMemberId());
      if (profile != null) {
        EnrichedPageView enrichedPageView =
            new EnrichedPageView(pageView.getPageKey(), profile.getMemberId(), profile.getCompany());
        collector.send(new OutgoingMessageEnvelope(OUTPUT_SYSTEM_STREAM, enrichedPageView));
      }
    }
  }

  static class PageViewProfileJoin implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      Table<KV<Integer, TestTableData.Profile>> table = appDescriptor.getTable(getTableDescriptor());
      DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor(SYSTEM_NAME);
      appDescriptor.getInputStream(sd.getInputDescriptor(PAGEVIEW_STREAM, new NoOpSerde<TestTableData.PageView>()))
          .partitionBy(TestTableData.PageView::getMemberId, v -> v,
              KVSerde.of(new IntegerSerde(), new TestTableData.PageViewJsonSerde()), "partition-page-view")
          .join(table, new PageViewToProfileJoinFunction())
          .sendTo(appDescriptor.getOutputStream(sd.getOutputDescriptor(ENRICHED_PAGEVIEW_STREAM, new NoOpSerde<>())));
    }

    protected TableDescriptor<Integer, Profile, ?> getTableDescriptor() {
      return new InMemoryTableDescriptor<>(PROFILE_TABLE, KVSerde.of(new IntegerSerde(), new ProfileJsonSerde()))
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
      return new RocksDbTableDescriptor<>(PROFILE_TABLE, KVSerde.of(new IntegerSerde(), new ProfileJsonSerde()))
          .withSideInputs(ImmutableList.of(PROFILE_STREAM))
          .withSideInputsProcessor((msg, store) -> {
            TestTableData.Profile profile = (TestTableData.Profile) msg.getMessage();
            int key = profile.getMemberId();
            return ImmutableList.of(new Entry<>(key, profile));
          });
    }
  }
}
