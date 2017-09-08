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
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;

/**
 * A {@link StreamApplication} that demonstrates a repartition followed by a windowed count.
 */
public class RepartitionWindowApp implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(RepartitionWindowApp.class);

  @Override
  public void init(StreamGraph graph, Config config) {
    graph.setDefaultKeySerde(new StringSerde());
    // will fail with class cast in partitionBy if (optional) PageView.class param not provided
    graph.setDefaultMsgSerde(new JsonSerde<>(PageView.class));

    MessageStream<PageView> pageViews = graph.getInputStream("page-views", (k, v) -> (PageView) v);

    OutputStream<String, String, WindowPane<String, Collection<PageView>>> outputStream =
        graph.getOutputStream(TestRepartitionWindowApp.OUTPUT_TOPIC,
            new StringSerde(), new StringSerde(),
            m -> m.getKey().getKey(), m -> Integer.toString(m.getMessage().size()));

    pageViews
        .partitionBy(PageView::getUserId)
        .window(Windows.keyedSessionWindow(PageView::getUserId, Duration.ofSeconds(3)))
        .sendTo(outputStream);
  }
}
