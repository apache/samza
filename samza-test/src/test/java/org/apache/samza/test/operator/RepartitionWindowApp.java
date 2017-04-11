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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link StreamApplication} that demonstrates a repartition followed by a windowed count.
 */
public class RepartitionWindowApp implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(RepartitionWindowApp.class);

  @Override
  public void init(StreamGraph graph, Config config) {

    BiFunction<String, String, String> msgBuilder = (k, v) -> v;
    MessageStream<String> pageViews = graph.getInputStream("page-views", msgBuilder);
    Function<String, String> keyFn = pageView -> new PageView(pageView).getUserId();

    OutputStream<String, String, WindowPane<String, Collection<String>>> outputStream = graph
        .getOutputStream(TestRepartitionWindowApp.OUTPUT_TOPIC, m -> m.getKey().getKey(), m -> new Integer(m.getMessage().size()).toString());

    pageViews
        .map(m -> m)
        .partitionBy(keyFn)
        .window(Windows.keyedSessionWindow(keyFn, Duration.ofSeconds(3)))
        // emit output
        .sendTo(outputStream);
  }
}
