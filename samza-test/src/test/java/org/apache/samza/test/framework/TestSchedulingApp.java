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

package org.apache.samza.test.framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.operators.Scheduler;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.test.operator.data.PageView;

public class TestSchedulingApp implements StreamApplication {
  public static final String PAGE_VIEWS = "page-views";

  @Override
  public void describe(StreamApplicationDescriptor appDesc) {
    final JsonSerdeV2<PageView> serde = new JsonSerdeV2<>(PageView.class);
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("kafka");
    KafkaInputDescriptor<PageView> isd = ksd.getInputDescriptor(PAGE_VIEWS, serde);
    final MessageStream<PageView> pageViews = appDesc.getInputStream(isd);
    final MessageStream<PageView> output = pageViews.flatMap(new FlatmapScheduledFn());

    MessageStreamAssert.that("Output from scheduling function should container all complete messages", output, serde)
        .containsInAnyOrder(
            Arrays.asList(
                new PageView("v1-complete", "p1", "u1"),
                new PageView("v2-complete", "p2", "u1"),
                new PageView("v3-complete", "p1", "u2"),
                new PageView("v4-complete", "p3", "u2")
            ));
  }

  private static class FlatmapScheduledFn
      implements FlatMapFunction<PageView, PageView>, ScheduledFunction<String, PageView> {

    private transient List<PageView> pageViews;
    private transient Scheduler<String> scheduler;

    @Override
    public void schedule(Scheduler<String> scheduler) {
      this.scheduler = scheduler;
      this.pageViews = new ArrayList<>();
    }

    @Override
    public Collection<PageView> apply(PageView message) {
      final PageView pv = new PageView(message.getViewId() + "-complete", message.getPageId(), message.getUserId());
      pageViews.add(pv);

      if (pageViews.size() == 2) {
        //got all messages for this task
        final long time = System.currentTimeMillis() + 100;
        scheduler.schedule("CompleteScheduler", time);
      }
      return Collections.emptyList();
    }

    @Override
    public Collection<PageView> onCallback(String key, long time) {
      return pageViews;
    }
  }
}
