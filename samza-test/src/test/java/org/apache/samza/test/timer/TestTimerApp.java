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

package org.apache.samza.test.timer;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.test.operator.data.PageView;
import com.apache.samza.test.framework.StreamAssert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TestTimerApp implements StreamApplication {
  public static final String PAGE_VIEWS = "page-views";

  @Override
  public void init(StreamGraph graph, Config config) {
    final JsonSerdeV2<PageView> serde = new JsonSerdeV2<>(PageView.class);
    final MessageStream<PageView> pageViews = graph.getInputStream(PAGE_VIEWS, serde);
    final MessageStream<PageView> output = pageViews.flatMap(new FlatmapTimerFn());

    StreamAssert.that("Output from timer function should container all complete messages", output, serde)
        .containsInAnyOrder(
            Arrays.asList(
                new PageView("v1-complete", "p1", "u1"),
                new PageView("v2-complete", "p2", "u1"),
                new PageView("v3-complete", "p1", "u2"),
                new PageView("v4-complete", "p3", "u2")
            ));
  }

  private static class FlatmapTimerFn implements FlatMapFunction<PageView, PageView>, TimerFunction<String, PageView> {

    private List<PageView> pageViews = new ArrayList<>();
    private TimerRegistry<String> timerRegistry;

    @Override
    public void registerTimer(TimerRegistry<String> timerRegistry) {
      this.timerRegistry = timerRegistry;
    }

    @Override
    public Collection<PageView> apply(PageView message) {
      final PageView pv = new PageView(message.getViewId() + "-complete", message.getPageId(), message.getUserId());
      pageViews.add(pv);

      if (pageViews.size() == 2) {
        //got all messages for this task
        final long time = System.currentTimeMillis() + 100;
        timerRegistry.register("CompleteTimer", time);
      }
      return Collections.emptyList();
    }

    @Override
    public Collection<PageView> onTimer(String key, long time) {
      return pageViews;
    }
  }
}
