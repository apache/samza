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
import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.test.framework.stream.CollectionStream;
import static org.apache.samza.test.controlmessages.TestData.PageView;
import org.junit.Assert;
import org.junit.Test;


public class StreamApplicationIntegrationTest {

  final StreamApplication app = (streamGraph, cfg) -> {
    streamGraph.<KV<String, PageView>>getInputStream("PageView")
        .map(Values.create())
        .partitionBy(pv -> pv.getMemberId(), pv -> pv, "p1")
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
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = i;
      pageviews.add(new PageView(pagekey, memberId));
    }

    CollectionStream<PageView> input = CollectionStream.of("test", "PageView", pageviews);
    CollectionStream output = CollectionStream.empty("test", "Output", 10);

    TestRunner
        .of(app)
        .addInputStream(input)
        .addOutputStream(output)
        .addOverrideConfig("job.default.system", "test")
        .run(Duration.ofMillis(1500));

    Assert.assertEquals(TestRunner.consumeStream(output, 10000).get(random.nextInt(count)).size(), 1);
  }

  public static final class Values {
    public static <K, V, M extends KV<K, V>> MapFunction<M, V> create() {
      return (M m) -> m.getValue();
    }
  }
}
