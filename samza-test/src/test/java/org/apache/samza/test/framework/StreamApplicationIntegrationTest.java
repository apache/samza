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
import java.util.List;
import java.util.Random;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.test.controlmessages.TestData;
import org.apache.samza.test.framework.system.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.controlmessages.TestData.PageView;

public class StreamApplicationIntegrationTest {
  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

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

  private static class PageViewFilterApplication implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDesc) {
      KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");
      KafkaInputDescriptor<KV<String, PageView>> isd =
          ksd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
      MessageStream<KV<String, TestData.PageView>> inputStream = appDesc.getInputStream(isd);
      inputStream.map(KV::getValue).filter(pv -> pv.getPageKey().equals("inbox"));
    }
  }

  private static class PageViewRepartitionApplication implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDesc) {
      KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");
      KafkaInputDescriptor<KV<String, PageView>> isd =
          ksd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
      MessageStream<KV<String, TestData.PageView>> inputStream = appDesc.getInputStream(isd);
      inputStream
          .map(KV::getValue)
          .partitionBy(PageView::getMemberId, pv -> pv, KVSerde.of(new IntegerSerde(), new JsonSerdeV2<>(PageView.class)), "p1")
          .sink((m, collector, coordinator) ->
              collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "Output"), m.getKey(), m.getKey(), m)));
    }
  }
}
