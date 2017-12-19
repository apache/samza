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
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.table.Table;

import samza.examples.cookbook.data.PageView;
import samza.examples.cookbook.data.Profile;

/**
 * In this example, we join a stream of Page views with a table of user profiles, which is populated from an
 * user profile stream. For instance, this is helpful for analysis that required additional information from
 * user's profile.
 *
 * <p> Concepts covered: Performing stream-to-table joins.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topics "pageview-join-input", "profile-table-input" are created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic pageview-join-input --partitions 2 --replication-factor 1
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic profile-table-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/pageview-profile-table-joiner.properties
 *   </li>
 *   <li>
 *     Produce some messages to the "profile-table-input" topic with the same userId <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic profile-table-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "company": "LNKD"} <br/>
 *     {"userId": "user2", "company": "MSFT"}
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-join-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-join-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com"}
 *   </li>
 *   <li>
 *     Consume messages from the "enriched-pageview-join-output" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic enriched-pageview-join-output
 *   </li>
 * </ol>
 *
 */
public class PageViewProfileTableJoiner implements StreamApplication {

  private static final String PROFILE_TOPIC = "profile-table-input";
  private static final String PAGEVIEW_TOPIC = "pageview-join-input";
  private static final String OUTPUT_TOPIC = "enriched-pageview-join-output";

  @Override
  public void init(StreamGraph graph, Config config) {

    Serde<Profile> profileSerde = new JsonSerdeV2<>(Profile.class);
    Serde<PageView> pageViewSerde = new JsonSerdeV2<>(PageView.class);

    OutputStream<EnrichedPageView> joinResultStream = graph.getOutputStream(
        OUTPUT_TOPIC, new JsonSerdeV2<>(EnrichedPageView.class));

    Table profileTable = graph.getTable(new RocksDbTableDescriptor<String, Profile>("profile-table")
        .withSerde(KVSerde.of(new StringSerde(), profileSerde)));

    graph.getInputStream(PROFILE_TOPIC, profileSerde)
        .map(profile -> KV.of(profile.userId, profile))
        .sendTo(profileTable);

    graph.getInputStream(PAGEVIEW_TOPIC, pageViewSerde)
        .partitionBy(pv -> pv.userId, pv -> pv, new KVSerde(new StringSerde(), pageViewSerde), "join")
        .join(profileTable, new JoinFn())
        .sendTo(joinResultStream);
  }

  private class JoinFn implements StreamTableJoinFunction<String, KV<String, PageView>, KV<String, Profile>, EnrichedPageView> {
    @Override
    public EnrichedPageView apply(KV<String, PageView> message, KV<String, Profile> record) {
      return record == null ? null :
          new EnrichedPageView(message.getKey(), record.getValue().company, message.getValue().pageId);
    }
    @Override
    public String getMessageKey(KV<String, PageView> message) {
      return message.getKey();
    }
    @Override
    public String getRecordKey(KV<String, Profile> record) {
      return record.getKey();
    }
  }

  static public class EnrichedPageView {

    public final String userId;
    public final String company;
    public final String pageId;

    public EnrichedPageView(String userId, String company, String pageId) {
      this.userId = userId;
      this.company = company;
      this.pageId = pageId;
    }
  }

}
