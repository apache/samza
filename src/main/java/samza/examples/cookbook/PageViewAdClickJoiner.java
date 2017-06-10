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
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.JoinFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Function;

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
 *     ./kafka-topics.sh  --zookeeper localhost:2181 --create --topic pageview-join-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the ./bin/run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory <br/>
 *     --config-path=file://$PWD/deploy/samza/config/pageview-adclick-joiner.properties)
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-join-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-join-input --broker-list localhost:9092 <br/>
 *     user1,india,google.com <br/>
 *     user2,china,yahoo.com
 *   </li>
 *   <li>
 *     Produce some messages to the "adclick-join-input" topic with the same pageKey <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic adclick-join-input --broker-list localhost:9092 <br/>
 *     adClickId1,user1,google.com <br/>
 *     adClickId2,user1,yahoo.com
 *   </li>
 *   <li>
 *     Consume messages from the "pageview-adclick-join-output" topic (e.g. bin/kafka-console-consumer.sh)
 *     ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pageview-adclick-join-output <br/>
 *     --property print.key=true
 *   </li>
 * </ol>
 *
 */
public class PageViewAdClickJoiner implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(PageViewAdClickJoiner.class);
  private static final String INPUT_TOPIC1 = "pageview-join-input";
  private static final String INPUT_TOPIC2 = "adclick-join-input";

  private static final String OUTPUT_TOPIC = "pageview-adclick-join-output";

  @Override
  public void init(StreamGraph graph, Config config) {

    MessageStream<String> pageViews = graph.<String, String, String>getInputStream(INPUT_TOPIC1, (k, v) -> v);
    MessageStream<String> adClicks = graph.<String, String, String>getInputStream(INPUT_TOPIC2, (k, v) -> v);

    OutputStream<String, String, String> outputStream = graph
        .getOutputStream(OUTPUT_TOPIC, m -> "", m -> m);

    Function<String, String> pageViewKeyFn = pageView -> new PageView(pageView).getPageId();
    Function<String, String> adClickKeyFn = adClick -> new AdClick(adClick).getPageId();

    MessageStream<String> pageViewRepartitioned = pageViews.partitionBy(pageViewKeyFn);
    MessageStream<String> adClickRepartitioned = adClicks.partitionBy(adClickKeyFn);

    pageViewRepartitioned.join(adClickRepartitioned, new JoinFunction<String, String, String, String>() {

      @Override
      public String apply(String pageViewMsg, String adClickMsg) {
        PageView pageView = new PageView(pageViewMsg);
        AdClick adClick = new AdClick(adClickMsg);
        String joinResult = String.format("%s,%s,%s", pageView.getPageId(), pageView.getCountry(), adClick.getAdId());
        return joinResult;
      }

      @Override
      public String getFirstKey(String msg) {
        return new PageView(msg).getPageId();
      }

      @Override
      public String getSecondKey(String msg) {
        return new AdClick(msg).getPageId();
      }
    }, Duration.ofMinutes(3)).sendTo(outputStream);
  }
}
