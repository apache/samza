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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.test.controlmessages.TestData;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.controlmessages.TestData.PageView;


public class StreamApplicationIntegrationTest {

  final StreamApplication pageViewFilter = (streamGraph, cfg) -> {
    GenericSystemDescriptor ksd = new GenericSystemDescriptor("test");
    GenericInputDescriptor<KV<String, PageView>> isd =
        ksd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
    MessageStream<KV<String, TestData.PageView>> inputStream = streamGraph.getInputStream(isd);
    inputStream.map(StreamApplicationIntegrationTest.Values.create()).filter(pv -> pv.getPageKey().equals("inbox"));
  };

  final StreamApplication pageViewRepartition = (streamGraph, cfg) -> {
    GenericSystemDescriptor ksd = new GenericSystemDescriptor("test");
    GenericInputDescriptor<KV<String, PageView>> isd =
        ksd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
    MessageStream<KV<String, TestData.PageView>> inputStream = streamGraph.getInputStream(isd);
    inputStream
        .map(Values.create())
        .partitionBy(PageView::getMemberId, pv -> pv, "p1")
        .sink((m, collector, coordinator) -> {
            collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "Output"),
                m.getKey(), m.getKey(),
                m));
          });
  };

  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

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

    CollectionStream<PageView> input = CollectionStream.of("test", "PageView", pageviews);
    CollectionStream output = CollectionStream.empty("test", "Output", 10);

    TestRunner
        .of(pageViewRepartition)
        .addInputStream(input)
        .addOutputStream(output)
        .addOverrideConfig("job.default.system", "test")
        .run(Duration.ofMillis(1500));

    Assert.assertEquals(TestRunner.consumeStream(output, Duration.ofMillis(1000)).get(random.nextInt(count)).size(), 1);
  }

  public static final class Values {
    public static <K, V, M extends KV<K, V>> MapFunction<M, V> create() {
      return (M m) -> m.getValue();
    }
  }

  /**
   * Job should fail since it is missing config "job.default.system" for partitionBy Operator
   */
  @Test(expected = SamzaException.class)
  public void testSamzaJobStartMissingConfigFailureForStreamApplication() {

    CollectionStream<TestData.PageView> input = CollectionStream.of("test", "PageView", new ArrayList<>());
    CollectionStream output = CollectionStream.empty("test", "Output", 10);

    TestRunner
        .of(pageViewRepartition)
        .addInputStream(input)
        .addOutputStream(output)
        .run(Duration.ofMillis(1000));
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

    CollectionStream<TestData.PageView> input = CollectionStream.of("test", "PageView", pageviews);
    CollectionStream output = CollectionStream.empty("test", "Output", 1);

    TestRunner.of(pageViewFilter)
        .addInputStream(input)
        .addOutputStream(output)
        .addOverrideConfig("job.default.system", "test")
        .run(Duration.ofMillis(1000));
  }

}
