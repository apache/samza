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

package samza.examples.wikipedia.task;

import java.util.Map;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.wikipedia.model.WikipediaParser;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;

public class WikipediaParserStreamTask implements StreamTask {
  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "wikipedia-edits");

  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    Map<String, Object> jsonObject = (Map<String, Object>) envelope.getMessage();
    WikipediaFeedEvent event = new WikipediaFeedEvent(jsonObject);

    Map<String, Object> parsedJsonObject = WikipediaParser.parseEvent(event);

    if (parsedJsonObject != null) {
      collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, parsedJsonObject));
    }
  }

  public static void main(String[] args) {
    String[] lines = new String[] { "[[Wikipedia talk:Articles for creation/Lords of War]]  http://en.wikipedia.org/w/index.php?diff=562991653&oldid=562991567 * BBGLordsofWar * (+95) /* Lords of War: Elves versus Lizardmen */]", "[[David Shepard (surgeon)]] M http://en.wikipedia.org/w/index.php?diff=562993463&oldid=562989820 * Jacobsievers * (+115) /* American Revolution (1775�1783) */  Added to note regarding David Shepard's brothers" };

    for (String line : lines) {
      System.out.println(WikipediaParser.parseLine(line));
    }
  }
}
