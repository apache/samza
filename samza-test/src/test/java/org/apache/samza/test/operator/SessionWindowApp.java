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
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Function;

/**
 * A {@link StreamApplication} that demonstrates a filter followed by a session window.
 */
public class SessionWindowApp implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(SessionWindowApp.class);
  private static final String FILTER_KEY = "badKey";

  @Override
  public void init(StreamGraph graph, Config config) {
    StreamSpec pageViewStreamSpec = new StreamSpec("page-views", TestRepartitionWindowApp.INPUT_TOPIC, "kafka");
    Function<MessageEnvelope<String, String>, String> keyFn = m -> m.getKey();

    MessageStream<MessageEnvelope<String, String>> pageViews = graph.createInStream(pageViewStreamSpec, null, null);
    pageViews
        .filter(m -> {
            LOG.info("Processing message with key: {} ", m.getKey());
            return !FILTER_KEY.equals(m.getKey());
          })
        // identity map
        .map(m -> m)
        // emit output
        .window(Windows.keyedSessionWindow(keyFn, Duration.ofSeconds(3)))
        .sink((windowOutput, collector, coordinator) -> {
            String key = windowOutput.getKey().getKey();
            String count = new Integer(windowOutput.getMessage().size()).toString();
            LOG.info("Count is " + count  + " for userId " + key);
            collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", TestRepartitionWindowApp.OUTPUT_TOPIC), key, count));
          });
  }
}
