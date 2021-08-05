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
package org.apache.samza.test.operator;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableList;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericOutputDescriptor;
import org.apache.samza.test.framework.StreamAssert;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.operator.data.PageView;
import org.junit.Test;


/**
 * Test driver for {@link RepartitionWindowApp}.
 */
public class TestRepartitionWindowApp {
  private static final String SYSTEM = "test";
  private static final String INPUT_TOPIC = "page-views";
  private static final String OUTPUT_TOPIC = "Result";

  @Test
  public void testRepartitionedSessionWindowCounter() throws Exception {
    Map<Integer, List<KV<String, PageView>>> pageViews = new HashMap<>();
    pageViews.put(0, ImmutableList.of(KV.of("userId1", new PageView("india", "5.com", "userId1")),
        KV.of("userId1", new PageView("india", "2.com", "userId1"))));
    pageViews.put(1, ImmutableList.of(KV.of("userId2", new PageView("china", "4.com", "userId2")),
        KV.of("userId1", new PageView("india", "3.com", "userId1"))));
    pageViews.put(2, ImmutableList.of(KV.of("userId1", new PageView("india", "1.com", "userId1"))));

    InMemorySystemDescriptor sd = new InMemorySystemDescriptor(SYSTEM);
    InMemoryInputDescriptor<KV<String, PageView>> inputDescriptor =
        sd.getInputDescriptor(INPUT_TOPIC, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
    /*
     * Technically, this should have a message type of KV, because a KV is passed to sendTo, but
     * StreamAssert.containsInAnyOrder requires the type to match the output type of the actual messages. In
     * high-level, sendTo splits up the KV, so the actual messages are just the "V" part of the KV.
     * TestRunner only uses NoOpSerde anyways, so it doesn't matter if the typing isn't KV.
     */
    InMemoryOutputDescriptor<String> outputDescriptor = sd.getOutputDescriptor(OUTPUT_TOPIC, new NoOpSerde<>());
    TestRunner.of(new RepartitionWindowApp())
        .addInputStream(inputDescriptor, pageViews)
        .addOutputStream(outputDescriptor, 1)
        .addConfig("task.window.ms", "1000")
        .run(Duration.ofSeconds(10));

    StreamAssert.containsInAnyOrder(Arrays.asList("userId1 4", "userId2 1"), outputDescriptor,
        Duration.ofSeconds(1));
  }

  /**
   * A {@link StreamApplication} that demonstrates a repartition followed by a windowed count.
   */
  private static class RepartitionWindowApp implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor(SYSTEM);
      GenericInputDescriptor<KV<String, PageView>> inputDescriptor =
          sd.getInputDescriptor(INPUT_TOPIC, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
      GenericOutputDescriptor<KV<String, String>> outputDescriptor =
          sd.getOutputDescriptor(OUTPUT_TOPIC, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));

      appDescriptor.getInputStream(inputDescriptor)
          .map(KV::getValue)
          .partitionBy(PageView::getUserId, m -> m, KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()), "p1")
          .window(Windows.keyedSessionWindow(KV::getKey, Duration.ofSeconds(3), () -> 0, (m, c) -> c + 1, new StringSerde("UTF-8"), new IntegerSerde()), "w1")
          .map(wp -> KV.of(wp.getKey().getKey(), wp.getKey().getKey() + " " + wp.getMessage()))
          .sendTo(appDescriptor.getOutputStream(outputDescriptor));
    }
  }
}
