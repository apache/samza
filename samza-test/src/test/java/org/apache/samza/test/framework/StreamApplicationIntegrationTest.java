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
package org.apache.samza.test.framework;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.test.controlmessages.TestData;
import org.apache.samza.test.framework.system.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.InMemorySystemDescriptor;
import org.apache.samza.test.table.PageViewToProfileJoinFunction;
import org.apache.samza.test.table.TestTableData;
import org.junit.Assert;
import org.junit.Test;
import static org.apache.samza.test.controlmessages.TestData.*;

public class StreamApplicationIntegrationTest {

  final StreamApplication pageViewFilter = streamAppDesc -> {
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");
    KafkaInputDescriptor<KV<String, PageView>> isd =
        ksd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
    MessageStream<KV<String, TestData.PageView>> inputStream = streamAppDesc.getInputStream(isd);
    inputStream.map(StreamApplicationIntegrationTest.Values.create()).filter(pv -> pv.getPageKey().equals("inbox"));
  };

  final StreamApplication pageViewRepartition = streamAppDesc -> {
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");
    KafkaInputDescriptor<KV<String, PageView>> isd =
        ksd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
    MessageStream<KV<String, TestData.PageView>> inputStream = streamAppDesc.getInputStream(isd);
    inputStream
        .map(Values.create())
        .partitionBy(PageView::getMemberId, pv -> pv, "p1")
        .sink((m, collector, coordinator) -> {
            collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "Output"),
                m.getKey(), m.getKey(),
                m));
          });
  };

  final StreamApplication pageViewProfileViewTableJoin = appDesc -> {
    Table<KV<Integer, TestTableData.Profile>> table = appDesc.getTable(
        new RocksDbTableDescriptor<Integer, TestTableData.Profile>("profile-view-store",
            KVSerde.of(new IntegerSerde(), new TestTableData.ProfileJsonSerde())));

    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");
    KafkaInputDescriptor<TestTableData.Profile> profileISD = ksd.getInputDescriptor("Profile", new NoOpSerde<>());
    appDesc.getInputStream(profileISD).map(m -> new KV(m.getMemberId(), m)).sendTo(table);

    KafkaInputDescriptor<TestTableData.PageView> pageViewISD = ksd.getInputDescriptor("PageView", new NoOpSerde<>());
    KafkaOutputDescriptor<TestTableData.EnrichedPageView> enrichedPageViewOSD =
        ksd.getOutputDescriptor("EnrichedPageView", new NoOpSerde<>());
    OutputStream<TestTableData.EnrichedPageView> outputStream = appDesc.getOutputStream(enrichedPageViewOSD);
    appDesc.getInputStream(pageViewISD)
        .partitionBy(TestTableData.PageView::getMemberId, v -> v, "p1")
        .join(table, new PageViewToProfileJoinFunction())
        .sendTo(outputStream);
  };

  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};


  @Test
  public void testStatefulJoinWithLocalTable() {
    List<TestTableData.PageView> pageViews = Arrays.asList(TestTableData.generatePageViews(10));
    List<TestTableData.Profile> profiles = Arrays.asList(TestTableData.generateProfiles(10));

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<TestTableData.PageView> pageViewStreamDesc = isd
        .getInputDescriptor("PageView", new NoOpSerde<TestTableData.PageView>());

    InMemoryInputDescriptor<TestTableData.Profile> profileStreamDesc = isd
        .getInputDescriptor("Profile", new NoOpSerde<TestTableData.Profile>())
        .withBootstrap(true);

    InMemoryOutputDescriptor<TestTableData.EnrichedPageView> outputStreamDesc = isd
        .getOutputDescriptor("EnrichedPageView", new NoOpSerde<>());

    TestRunner
        .of(pageViewProfileViewTableJoin)
        .addInputStream(pageViewStreamDesc, pageViews)
        .addInputStream(profileStreamDesc, profiles)
        .addOutputStream(outputStreamDesc, 1)
        .run(Duration.ofSeconds(2));

    Assert.assertEquals(10, TestRunner.consumeStream(outputStreamDesc, Duration.ofSeconds(1)).get(0).size());
  }

  @Test
  public void testHighLevelApi() throws Exception {
    Random random = new Random();
    int count = 10;
    List<PageView> pageviews = new ArrayList<>(count);
    Map<Integer, List<PageView>> expectedOutput = new HashMap<>();
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = i;
      PageView pv = new PageView(pagekey, memberId);
      pageviews.add(pv);
    }

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<PageView> imid = isd
        .getInputDescriptor("PageView", new NoOpSerde<PageView>());

    InMemoryOutputDescriptor<PageView> imod = isd
        .getOutputDescriptor("Output", new NoOpSerde<PageView>());

    TestRunner
        .of(pageViewRepartition)
        .addInputStream(imid, pageviews)
        .addOutputStream(imod, 10)
        .run(Duration.ofMillis(1500));

    Assert.assertEquals(TestRunner.consumeStream(imod, Duration.ofMillis(1000)).get(random.nextInt(count)).size(), 1);
  }

  public static final class Values {
    public static <K, V, M extends KV<K, V>> MapFunction<M, V> create() {
      return (M m) -> m.getValue();
    }
  }


  /**
   * Null page key is passed in input data which should fail filter logic
   */
  @Test(expected = SamzaException.class)
  public void testSamzaJobFailureForStreamApplication() {
    Random random = new Random();
    int count = 10;
    List<TestData.PageView> pageviews = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = i;
      pageviews.add(new TestData.PageView(null, memberId));
    }

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<PageView> imid = isd
        .getInputDescriptor("PageView", new NoOpSerde<PageView>());

    InMemoryOutputDescriptor<PageView> imod = isd
        .getOutputDescriptor("Output", new NoOpSerde<PageView>());

    TestRunner.of(pageViewFilter)
        .addInputStream(imid, pageviews)
        .addOutputStream(imod, 10)
        .run(Duration.ofMillis(1000));
  }

}
