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
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.test.controlmessages.TestData;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.table.PageViewToProfileJoinFunction;
import org.apache.samza.test.table.TestTableData;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.controlmessages.TestData.PageView;

public class StreamApplicationIntegrationTest {

  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

  @Test
  public void testStatefulJoinWithLocalTable() {
    Random random = new Random();
    List<KV<String, TestTableData.PageView>> pageViews = Arrays.asList(TestTableData.generatePageViews(10))
        .stream()
        .map(x -> KV.of(PAGEKEYS[random.nextInt(PAGEKEYS.length)], x))
        .collect(Collectors.toList());
    List<KV<String, TestTableData.Profile>> profiles = Arrays.asList(TestTableData.generateProfiles(10))
        .stream()
        .map(x -> KV.of(PAGEKEYS[random.nextInt(PAGEKEYS.length)], x))
        .collect(Collectors.toList());

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<KV<String, TestTableData.PageView>> pageViewStreamDesc = isd
        .getInputDescriptor("PageView", new NoOpSerde<KV<String, TestTableData.PageView>>());

    InMemoryInputDescriptor<KV<String, TestTableData.Profile>> profileStreamDesc = isd
        .getInputDescriptor("Profile", new NoOpSerde<KV<String, TestTableData.Profile>>())
        .shouldBootstrap();

    InMemoryOutputDescriptor<TestTableData.EnrichedPageView> outputStreamDesc = isd
        .getOutputDescriptor("EnrichedPageView", new NoOpSerde<>());

    TestRunner
        .of(new PageViewProfileViewJoinApplication())
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
    List<PageView> pageViews = new ArrayList<>();
    for (int memberId = 0; memberId < count; memberId++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      PageView pv = new PageView(pagekey, memberId);
      pageViews.add(pv);
    }

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<PageView> imid = isd.getInputDescriptor("PageView", new NoOpSerde<PageView>());
    InMemoryOutputDescriptor<PageView> imod = isd.getOutputDescriptor("Output", new NoOpSerde<PageView>());

    TestRunner
        .of(new PageViewRepartitionApplication())
        .addInputStream(imid, pageViews)
        .addOutputStream(imod, 10)
        .run(Duration.ofMillis(1500));

    Assert.assertEquals(TestRunner.consumeStream(imod, Duration.ofMillis(1000)).get(random.nextInt(count)).size(), 1);
  }

  /**
   * Null page key is passed in input data which should fail filter logic
   */
  @Test(expected = SamzaException.class)
  public void testSamzaJobFailureForStreamApplication() {
    int count = 10;
    List<TestData.PageView> pageviews = new ArrayList<>();
    for (int memberId = 0; memberId < count; memberId++) {
      pageviews.add(new TestData.PageView(null, memberId));
    }

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<PageView> imid = isd.getInputDescriptor("PageView", new NoOpSerde<PageView>());
    InMemoryOutputDescriptor<PageView> imod = isd.getOutputDescriptor("Output", new NoOpSerde<PageView>());

    TestRunner.of(new PageViewFilterApplication())
        .addInputStream(imid, pageviews)
        .addOutputStream(imod, 10)
        .run(Duration.ofMillis(1000));
  }


  private static class PageViewProfileViewJoinApplication implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      Table<KV<Integer, TestTableData.Profile>> table = appDescriptor.getTable(
          new RocksDbTableDescriptor<Integer, TestTableData.Profile>("profile-view-store",
              KVSerde.of(new IntegerSerde(), new TestTableData.ProfileJsonSerde())));

      KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");

      KafkaInputDescriptor<KV<String, TestTableData.Profile>> profileISD =
          ksd.getInputDescriptor("Profile", KVSerde.of(new StringSerde(), new JsonSerdeV2<>()));

      appDescriptor
          .getInputStream(profileISD)
          .map(m -> new KV(m.getValue().getMemberId(), m.getValue()))
          .sendTo(table);

      KafkaInputDescriptor<KV<String, TestTableData.PageView>> pageViewISD =
          ksd.getInputDescriptor("PageView", KVSerde.of(new StringSerde(), new JsonSerdeV2<>()));
      KafkaOutputDescriptor<TestTableData.EnrichedPageView> enrichedPageViewOSD =
          ksd.getOutputDescriptor("EnrichedPageView", new JsonSerdeV2<>());

      OutputStream<TestTableData.EnrichedPageView> outputStream = appDescriptor.getOutputStream(enrichedPageViewOSD);
      appDescriptor.getInputStream(pageViewISD)
          .partitionBy(pv -> pv.getValue().getMemberId(),  pv -> pv.getValue(), KVSerde.of(new IntegerSerde(), new JsonSerdeV2<>(TestTableData.PageView.class)), "p1")
          .join(table, new PageViewToProfileJoinFunction())
          .sendTo(outputStream);
    }
  }

  private static class PageViewFilterApplication implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");
      KafkaInputDescriptor<KV<String, PageView>> isd =
          ksd.getInputDescriptor("PageView", KVSerde.of(new StringSerde(), new JsonSerdeV2<>()));
      MessageStream<KV<String, TestData.PageView>> inputStream = appDescriptor.getInputStream(isd);
      inputStream.map(KV::getValue).filter(pv -> pv.getPageKey().equals("inbox"));
    }
  }

  private static class PageViewRepartitionApplication implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");
      KafkaInputDescriptor<KV<String, PageView>> isd =
          ksd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
      MessageStream<KV<String, TestData.PageView>> inputStream = appDescriptor.getInputStream(isd);
      inputStream
          .map(KV::getValue)
          .partitionBy(PageView::getMemberId, pv -> pv, KVSerde.of(new IntegerSerde(), new JsonSerdeV2<>(PageView.class)), "p1")
          .sink((m, collector, coordinator) ->
              collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "Output"), m.getKey(), m.getKey(), m)));
    }
  }
}
