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

import java.time.Duration;

/**
 * A {@link StreamApplication} that demonstrates a repartition followed by a windowed count.
 */
public class RepartitionWindowApp implements StreamApplication {
  static final String INPUT_TOPIC = "page-views";
  static final String OUTPUT_TOPIC = "page-view-counts";

  @Override
  public void init(StreamGraph graph, Config config) {
    MessageStream<PageView> pageViews = graph.getInputStream(INPUT_TOPIC, new JsonSerde<>(PageView.class));

    OutputStream<KV<String, String>> outputStream =
        graph.getOutputStream(OUTPUT_TOPIC, new KVSerde<>(new StringSerde(), new StringSerde()));

    pageViews
        .repartition(PageView::getUserId, pv -> pv, new KVSerde<>(new StringSerde(), new JsonSerde<>(PageView.class)))
        .window(Windows.keyedSessionWindow(KV::getKey, Duration.ofSeconds(3)))
        .map(windowPane -> KV.of(windowPane.getKey().getKey(), String.valueOf(windowPane.getMessage().size())))
        .sendTo(outputStream);
  }
}
