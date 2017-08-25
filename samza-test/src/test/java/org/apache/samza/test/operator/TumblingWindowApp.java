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

//import org.apache.samza.application.StreamApplication;
//import org.apache.samza.config.Config;
//import org.apache.samza.operators.MessageStream;
//import org.apache.samza.operators.OutputStream;
//import org.apache.samza.operators.StreamGraph;
//import org.apache.samza.operators.windows.WindowPane;
//import org.apache.samza.operators.windows.Windows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.time.Duration;
//import java.util.Collection;

/**
 * A {@link org.apache.samza.application.StreamApplication} that demonstrates a filter followed by a tumbling window.
 */
//public class TumblingWindowApp implements StreamApplication {
public class TumblingWindowApp {

  private static final Logger LOG = LoggerFactory.getLogger(TumblingWindowApp.class);
  private static final String FILTER_KEY = "badKey";
  private static final String OUTPUT_TOPIC = "Result";

//  @Override
//  public void init(StreamGraph graph, Config config) {
//    MessageStream<PageView> pageViews = graph.<String, String, PageView>getInputStream("page-views", (k, v) -> new PageView(v));
//    OutputStream<String, String, WindowPane<String, Collection<PageView>>> outputStream = graph
//        .getOutputStream(OUTPUT_TOPIC, m -> m.getKey().getKey(), m -> new Integer(m.getMessage().size()).toString());
//
//    pageViews
//        .filter(m -> !FILTER_KEY.equals(m.getUserId()))
//        .window(Windows.keyedTumblingWindow(pageView -> pageView.getUserId(), Duration.ofSeconds(3)))
//        .sendTo(outputStream);
//  }
}
