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
package samza.examples.cookbook;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import samza.examples.cookbook.data.AdClick;
import samza.examples.cookbook.data.PageView;

import java.time.Duration;

/**
 * In this example, we join a stream of Page views with a stream of Ad clicks. For instance, this is helpful for
 * analysis on what pages served an Ad that was clicked.
 *
 * <p> Concepts covered: Performing stream to stream Joins.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topics "pageview-join-input", "adclick-join-input" are created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic pageview-join-input --partitions 2 --replication-factor 1
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic adclick-join-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/pageview-adclick-joiner.properties
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-join-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-join-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com"}
 *   </li>
 *   <li>
 *     Produce some messages to the "adclick-join-input" topic with the same pageKey <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic adclick-join-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "adId": "adClickId1", "pageId":"google.com"} <br/>
 *     {"userId": "user1", "adId": "adClickId2", "pageId":"yahoo.com"}
 *   </li>
 *   <li>
 *     Consume messages from the "pageview-adclick-join-output" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pageview-adclick-join-output --property print.key=true
 *   </li>
 * </ol>
 *
 */
public class PageViewAdClickJoiner implements StreamApplication {

  private static final String PAGEVIEW_TOPIC = "pageview-join-input";
  private static final String AD_CLICK_TOPIC = "adclick-join-input";
  private static final String OUTPUT_TOPIC = "pageview-adclick-join-output";

  @Override
  public void init(StreamGraph graph, Config config) {
    StringSerde stringSerde = new StringSerde();
    JsonSerdeV2<PageView> pageViewSerde = new JsonSerdeV2<>(PageView.class);
    JsonSerdeV2<AdClick> adClickSerde = new JsonSerdeV2<>(AdClick.class);
    JsonSerdeV2<JoinResult> joinResultSerde = new JsonSerdeV2<>(JoinResult.class);

    MessageStream<PageView> pageViews = graph.getInputStream(PAGEVIEW_TOPIC, pageViewSerde);
    MessageStream<AdClick> adClicks = graph.getInputStream(AD_CLICK_TOPIC, adClickSerde);
    OutputStream<JoinResult> joinResults = graph.getOutputStream(OUTPUT_TOPIC, joinResultSerde);

    JoinFunction<String, PageView, AdClick, JoinResult> pageViewAdClickJoinFunction =
        new JoinFunction<String, PageView, AdClick, JoinResult>() {
          @Override
          public JoinResult apply(PageView pageView, AdClick adClick) {
            return new JoinResult(pageView.pageId, pageView.userId, pageView.country, adClick.getAdId());
          }

          @Override
          public String getFirstKey(PageView pageView) {
            return pageView.pageId;
          }

          @Override
          public String getSecondKey(AdClick adClick) {
            return adClick.getPageId();
          }
        };

    MessageStream<PageView> repartitionedPageViews =
        pageViews
            .partitionBy(pv -> pv.pageId, pv -> pv, KVSerde.of(stringSerde, pageViewSerde))
            .map(KV::getValue);

    MessageStream<AdClick> repartitionedAdClicks =
        adClicks
            .partitionBy(AdClick::getPageId, ac -> ac, KVSerde.of(stringSerde, adClickSerde))
            .map(KV::getValue);

    repartitionedPageViews
        .join(repartitionedAdClicks, pageViewAdClickJoinFunction,
            stringSerde, pageViewSerde, adClickSerde, Duration.ofMinutes(3))
        .sendTo(joinResults);
  }

  static class JoinResult {
    public String pageId;
    public String userId;
    public String country;
    public String adId;

    public JoinResult(String pageId, String userId, String country, String adId) {
      this.pageId = pageId;
      this.userId = userId;
      this.country = country;
      this.adId = adId;
    }
  }
}
