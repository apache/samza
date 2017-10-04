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
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import samza.examples.cookbook.data.PageView;
import samza.examples.cookbook.data.UserPageViews;

import java.time.Duration;

/**
 * In this example, we group page views by userId into sessions, and compute the number of page views for each user
 * session. A session is considered closed when there is no user activity for a 10 second duration.
 *
 * <p>Concepts covered: Using session windows to group data in a stream, Re-partitioning a stream.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topic "pageview-session-input" is created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic pageview-session-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/pageview-sessionizer.properties
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-session-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-session-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com/home"} <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com/search"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com/home"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com/sports"} <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com/news"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com/fashion"}
 *   </li>
 *   <li>
 *     Consume messages from the "pageview-session-output" topic (e.g. bin/kafka-console-consumer.sh)
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pageview-session-output --property print.key=true
 *   </li>
 * </ol>
 *
 */
public class PageViewSessionizerApp implements StreamApplication {

  private static final String INPUT_TOPIC = "pageview-session-input";
  private static final String OUTPUT_TOPIC = "pageview-session-output";

  @Override
  public void init(StreamGraph graph, Config config) {
    graph.setDefaultSerde(KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageView.class)));

    MessageStream<KV<String, PageView>> pageViews = graph.getInputStream(INPUT_TOPIC);
    OutputStream<KV<String, UserPageViews>> userPageViews =
        graph.getOutputStream(OUTPUT_TOPIC, KVSerde.of(new StringSerde(), new JsonSerdeV2<>(UserPageViews.class)));

    pageViews
        .partitionBy(kv -> kv.value.userId, kv -> kv.value)
        .window(Windows.keyedSessionWindow(kv -> kv.value.userId, Duration.ofSeconds(10)))
        .map(windowPane -> {
            String userId = windowPane.getKey().getKey();
            int views = windowPane.getMessage().size();
            return KV.of(userId, new UserPageViews(userId, views));
          })
        .sendTo(userPageViews);
  }
}
