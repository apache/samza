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
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import samza.examples.cookbook.data.PageView;

/**
 * In this example, we demonstrate re-partitioning a stream of page views and filtering out some bad events in the stream.
 *
 * <p>Concepts covered: Using stateless operators on a stream, Re-partitioning a stream.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topic "pageview-filter-input" is created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic pageview-filter-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/pageview-filter.properties
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-filter-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-filter-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com"} <br/>
 *     {"userId": "invalidUserId", "country": "france", "pageId":"facebook.com"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com"}
 *   </li>
 *   <li>
 *     Consume messages from the "pageview-filter-output" topic (e.g. bin/kafka-console-consumer.sh)
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pageview-filter-output --property print.key=true
 *   </li>
 * </ol>
 */
public class PageViewFilterApp implements StreamApplication {

  private static final String INPUT_TOPIC = "pageview-filter-input";
  private static final String OUTPUT_TOPIC = "pageview-filter-output";
  private static final String INVALID_USER_ID = "invalidUserId";

  @Override
  public void init(StreamGraph graph, Config config) {
    graph.setDefaultSerde(KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageView.class)));

    MessageStream<KV<String, PageView>> pageViews = graph.getInputStream(INPUT_TOPIC);
    OutputStream<KV<String, PageView>> filteredPageViews = graph.getOutputStream(OUTPUT_TOPIC);

    pageViews
        .partitionBy(kv -> kv.value.userId, kv -> kv.value)
        .filter(kv -> !INVALID_USER_ID.equals(kv.value.userId))
        .sendTo(filteredPageViews);
  }
}
