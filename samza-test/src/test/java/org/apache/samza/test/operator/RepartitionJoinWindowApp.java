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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.operator.data.AdClick;
import org.apache.samza.test.operator.data.PageView;
import org.apache.samza.test.operator.data.UserPageAdClick;


/**
 * A {@link StreamApplication} that demonstrates a partitionBy, stream-stream join and a windowed count.
 */
public class RepartitionJoinWindowApp implements StreamApplication {
  public static final String SYSTEM = "kafka";
  public static final String INPUT_TOPIC_1_CONFIG_KEY = "inputTopic1";
  public static final String INPUT_TOPIC_2_CONFIG_KEY = "inputTopic2";
  public static final String OUTPUT_TOPIC_CONFIG_KEY = "outputTopic";

  private final List<String> intermediateStreamIds = new ArrayList<>();

  @Override
  public void describe(StreamApplicationDescriptor appDesc) {
    // offset.default = oldest required for tests since checkpoint topic is empty on start and messages are published
    // before the application is run
    Config config = appDesc.getConfig();
    String inputTopic1 = config.get(INPUT_TOPIC_1_CONFIG_KEY);
    String inputTopic2 = config.get(INPUT_TOPIC_2_CONFIG_KEY);
    String outputTopic = config.get(OUTPUT_TOPIC_CONFIG_KEY);
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(SYSTEM);
    KafkaInputDescriptor<PageView> id1 = ksd.getInputDescriptor(inputTopic1, new JsonSerdeV2<>(PageView.class));
    KafkaInputDescriptor<AdClick> id2 = ksd.getInputDescriptor(inputTopic2, new JsonSerdeV2<>(AdClick.class));

    MessageStream<PageView> pageViews = appDesc.getInputStream(id1);
    MessageStream<AdClick> adClicks = appDesc.getInputStream(id2);

    MessageStream<KV<String, PageView>> pageViewsRepartitionedByViewId = pageViews
        .partitionBy(PageView::getViewId, pv -> pv,
            new KVSerde<>(new StringSerde(), new JsonSerdeV2<>(PageView.class)), "pageViewsByViewId");

    MessageStream<PageView> pageViewsRepartitionedByViewIdValueONly = pageViewsRepartitionedByViewId.map(KV::getValue);

    MessageStream<KV<String, AdClick>> adClicksRepartitionedByViewId = adClicks
        .partitionBy(AdClick::getViewId, ac -> ac,
            new KVSerde<>(new StringSerde(), new JsonSerdeV2<>(AdClick.class)), "adClicksByViewId");
    MessageStream<AdClick> adClicksRepartitionedByViewIdValueOnly = adClicksRepartitionedByViewId.map(KV::getValue);

    MessageStream<UserPageAdClick> userPageAdClicks = pageViewsRepartitionedByViewIdValueONly
        .join(adClicksRepartitionedByViewIdValueOnly, new UserPageViewAdClicksJoiner(),
            new StringSerde(), new JsonSerdeV2<>(PageView.class), new JsonSerdeV2<>(AdClick.class),
            Duration.ofMinutes(1), "pageViewAdClickJoin");

    MessageStream<KV<String, UserPageAdClick>> userPageAdClicksByUserId = userPageAdClicks
        .partitionBy(UserPageAdClick::getUserId, upac -> upac,
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(UserPageAdClick.class)), "userPageAdClicksByUserId");

    userPageAdClicksByUserId.map(KV::getValue)
        .window(Windows.keyedSessionWindow(UserPageAdClick::getUserId, Duration.ofSeconds(3),
            new StringSerde(), new JsonSerdeV2<>(UserPageAdClick.class)), "userAdClickWindow")
        .map(windowPane -> KV.of(windowPane.getKey().getKey(), String.valueOf(windowPane.getMessage().size())))
        .sink((message, messageCollector, taskCoordinator) -> {
            taskCoordinator.commit(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
            messageCollector.send(
                new OutgoingMessageEnvelope(
                    new SystemStream("kafka", outputTopic), null, message.getKey(), message.getValue()));
          });


    intermediateStreamIds.add(((IntermediateMessageStreamImpl) pageViewsRepartitionedByViewId).getStreamId());
    intermediateStreamIds.add(((IntermediateMessageStreamImpl) adClicksRepartitionedByViewId).getStreamId());
    intermediateStreamIds.add(((IntermediateMessageStreamImpl) userPageAdClicksByUserId).getStreamId());
  }

  List<String> getIntermediateStreamIds() {
    return intermediateStreamIds;
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
