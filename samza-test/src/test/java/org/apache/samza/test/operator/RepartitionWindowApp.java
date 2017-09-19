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
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * A {@link StreamApplication} that demonstrates a repartition followed by a windowed count.
 */
public class RepartitionWindowApp implements StreamApplication {
  private static final Logger LOGGER = LoggerFactory.getLogger(RepartitionWindowApp.class);

  @Override
  public void init(StreamGraph graph, Config config) {
    MessageStream<PageView> pageViews =
        graph.getInputStream("page-views", new JsonSerde<>(PageView.class));

    OutputStream<KV<String, String>> outputStream =
        graph.getOutputStream(TestRepartitionWindowApp.OUTPUT_TOPIC,
            new KVSerde<>(new StringSerde(), new StringSerde()));

    pageViews
        .repartition(PageView::getUserId, pageView -> pageView,
            new KVSerde<>(new StringSerde(), new JsonSerde<>(PageView.class)))
        .window(Windows.keyedSessionWindow(m -> m.getValue().getUserId(), Duration.ofSeconds(3)))
        .map(windowPane -> KV.of(windowPane.getKey().getKey(), String.valueOf(windowPane.getMessage().size())))
        .sendTo(outputStream);

//    pageViews
//        .repartition(PageView::getUserId, pageView -> pageView)
//        .window(Windows.keyedSessionWindow(Pair::getKey, Duration.ofSeconds(3)))
//        .map(windowPane -> new PageViewCount(windowPane.getKey().getKey(), windowPane.getMessage().size()))
//        .sendTo(outputStream); // bad idea in kafka

    // ===================================================
//
//    MessageStream<KV<String, PageView>> keyedPageViews = graph
//        .getInputStream("page-views", new KVSerde<>(new StringSerde(), new JsonSerde<>(PageView.class)));
//
//    OutputStream<KV<String, PageViewCount>> keyedOutputStream =
//        graph.getOutputStream(TestRepartitionWindowApp.OUTPUT_TOPIC,
//            new KVSerde<>(new StringSerde(), new JsonSerde<>(PageViewCount.class)));
//
//    keyedPageViews
//        .repartition(Pair::getKey, Pair::getValue, new KVSerde<>(new StringSerde(), new JsonSerde<>(PageView.class)))
//        .window(Windows.keyedSessionWindow(Pair::getKey, Duration.ofSeconds(3)))
//        .map(m -> Pair.of(m.getKey().getKey(), new PageViewCount(m.getKey().getKey(), m.getMessage().size())))
//        .sendTo(keyedOutputStream);
//
//    keyedPageViews
//        .repartition(Pair::getKey, Pair::getValue)
//        .window(Windows.keyedSessionWindow(Pair::getKey, Duration.ofSeconds(3)))
//        .map(m -> Pair.of(m.getKey().getKey(), new PageViewCount(m.getKey().getKey(), m.getMessage().size())))
//        .sendTo(keyedOutputStream);
  }

//  static class PageViewCount {
//    String userId;
//    int count;
//
//    public PageViewCount(String userId, int count) {
//      this.userId = userId;
//      this.count = count;
//    }
//
//    public String getUserId() {
//      return userId;
//    }
//
//    public void setUserId(String userId) {
//      this.userId = userId;
//    }
//
//    public int getCount() {
//      return count;
//    }
//
//    public void setCount(int count) {
//      this.count = count;
//    }
//  }
}
