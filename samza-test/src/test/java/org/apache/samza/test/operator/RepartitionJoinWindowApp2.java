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
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.operator.data.AdClick;
import org.apache.samza.test.operator.data.PageView;
import org.apache.samza.test.operator.data.UserPageAdClick;

import java.time.Duration;

/**
 * A {@link StreamApplication} that demonstrates a partitionBy, stream-stream join and a windowed count.
 */
public class RepartitionJoinWindowApp2 implements StreamApplication {
  static final String PAGE_VIEWS = "page-views2";
  static final String AD_CLICKS = "ad-clicks2";
  static final String OUTPUT_TOPIC = "user-ad-click-counts2";

  @Override
  public void init(StreamGraph graph, Config config) {
    MessageStream<PageView> pageViews = graph.getInputStream(PAGE_VIEWS, new JsonSerdeV2<>(PageView.class));
    MessageStream<AdClick> adClicks = graph.getInputStream(AD_CLICKS, new JsonSerdeV2<>(AdClick.class));

    MessageStream<PageView> pageViewsRepartitionedByViewId = pageViews
        .partitionBy(PageView::getViewId, pv -> pv,
            new KVSerde<>(new StringSerde(), new JsonSerdeV2<>(PageView.class)), "pageViewsByViewId2")
        .map(KV::getValue);

    MessageStream<AdClick> adClicksRepartitionedByViewId = adClicks
        .partitionBy(AdClick::getViewId, ac -> ac,
            new KVSerde<>(new StringSerde(), new JsonSerdeV2<>(AdClick.class)), "adClicksByViewId2")
        .map(KV::getValue);

    MessageStream<UserPageAdClick> userPageAdClicks = pageViewsRepartitionedByViewId
        .join(adClicksRepartitionedByViewId, new UserPageViewAdClicksJoiner(),
            new StringSerde(), new JsonSerdeV2<>(PageView.class), new JsonSerdeV2<>(AdClick.class),
            Duration.ofMinutes(1), "pageViewAdClickJoin2");

    userPageAdClicks
      .partitionBy(UserPageAdClick::getUserId, upac -> upac,
        KVSerde.of(new StringSerde(), new JsonSerdeV2<>(UserPageAdClick.class)), "userPageAdClicksByUserId2")
      .map(KV::getValue)
      .window(Windows.keyedSessionWindow(UserPageAdClick::getUserId, Duration.ofSeconds(3),
        new StringSerde(), new JsonSerdeV2<>(UserPageAdClick.class)), "userAdClickWindow2")
      .map(windowPane -> KV.of(windowPane.getKey().getKey(), String.valueOf(windowPane.getMessage().size())))
      .sink((message, messageCollector, taskCoordinator) -> {
          taskCoordinator.commit(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
          messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", OUTPUT_TOPIC), null, message.getKey(), message.getValue()));
        });
  }

  private static class UserPageViewAdClicksJoiner implements JoinFunction<String, PageView, AdClick, UserPageAdClick> {
    @Override
    public UserPageAdClick apply(PageView pv, AdClick ac) {
      return new UserPageAdClick(pv.getUserId(), pv.getPageId(), ac.getAdId());
    }

    @Override
    public String getFirstKey(PageView pv) {
      return pv.getViewId();
    }

    @Override
    public String getSecondKey(AdClick ac) {
      return ac.getViewId();
    }
  }
}
