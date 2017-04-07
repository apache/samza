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

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link StreamApplication} that demonstrates a repartition followed by a windowed count.
 */
public class RepartitionWindowApp implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(RepartitionWindowApp.class);

  @Override
  public void init(StreamGraph graph, Config config) {
    StreamSpec pageViewStreamSpec = new StreamSpec("page-views", TestRepartitionWindowApp.INPUT_TOPIC, "kafka");

    BiFunction<String, String, PageView> msgBuilder = (k, v) -> new PageView(v);
    MessageStream<PageView> pageViews = graph.getInputStream("page-views", msgBuilder);
    Function<PageView, String> keyFn = pageView -> pageView.getUserId();

    BiFunction<String, String, String> msgBuilder1 = (k, v) -> v;
    MessageStream<String> pageViews1 = graph.getInputStream("page-views", msgBuilder1);
    Function<String, String> keyFn1 = pageView -> new PageView(pageView).getUserId();
    /*
    pageViews1
        .map(m -> {
          System.out.println("got msg:" + m);
         return m;
        })
        //.partitionBy(keyFn1)
        .window(Windows.keyedSessionWindow(keyFn1, Duration.ofSeconds(3)))
        // emit output
        .sink((windowOutput, collector, coordinator) -> {
            String key = windowOutput.getKey().getKey();
            String count = new Integer(windowOutput.getMessage().size()).toString();
            LOG.info("Count is " + count  + " for key " + key);
            collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", TestRepartitionWindowApp.OUTPUT_TOPIC), key, count));
          });
          */


  }
}
