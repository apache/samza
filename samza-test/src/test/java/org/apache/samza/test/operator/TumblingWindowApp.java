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
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link StreamApplication} that demonstrates a filter followed by a tumbling window.
 */
public class TumblingWindowApp implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(TumblingWindowApp.class);
  private static final String FILTER_KEY = "badKey";

  @Override
  public void init(StreamGraph graph, Config config) {
    BiFunction<String, String, PageView> msgBuilder = (k, v) -> new PageView(v);
    MessageStream<PageView> pageViews = graph.getInputStream("page-views", msgBuilder);
    Function<PageView, String> keyFn = pageView -> pageView.getUserId();

    pageViews
        .filter(m -> !FILTER_KEY.equals(m.getUserId()))
        // identity map
        .map(m -> m)
        // emit output
        .window(Windows.keyedTumblingWindow(keyFn, TestTumblingWindowApp.DURATION))
        .sink((windowOutput, collector, coordinator) -> {
            String key = windowOutput.getKey().getKey();
            String count = new Integer(windowOutput.getMessage().size()).toString();
            collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", TestRepartitionWindowApp.OUTPUT_TOPIC), key, count));
          });
  }
}
