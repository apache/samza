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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;

public class WikipediaParserStreamTask implements StreamTask {
  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    Map<String, Object> jsonObject = (Map<String, Object>) envelope.getMessage();
    WikipediaFeedEvent event = new WikipediaFeedEvent(jsonObject);

    try {
      Map<String, Object> parsedJsonObject = parse(event.getRawEvent());

      parsedJsonObject.put("channel", event.getChannel());
      parsedJsonObject.put("source", event.getSource());
      parsedJsonObject.put("time", event.getTime());

      collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "wikipedia-edits"), parsedJsonObject));
    } catch (Exception e) {
      System.err.println("Unable to parse line: " + event);
    }
  }

  public static Map<String, Object> parse(String line) {
    Pattern p = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");
    Matcher m = p.matcher(line);

    if (m.find() && m.groupCount() == 6) {
      String title = m.group(1);
      String flags = m.group(2);
      String diffUrl = m.group(3);
      String user = m.group(4);
      int byteDiff = Integer.parseInt(m.group(5));
      String summary = m.group(6);

      Map<String, Boolean> flagMap = new HashMap<String, Boolean>();

      flagMap.put("is-minor", flags.contains("M"));
      flagMap.put("is-new", flags.contains("N"));
      flagMap.put("is-unpatrolled", flags.contains("!"));
      flagMap.put("is-bot-edit", flags.contains("B"));
      flagMap.put("is-special", title.startsWith("Special:"));
      flagMap.put("is-talk", title.startsWith("Talk:"));

      Map<String, Object> root = new HashMap<String, Object>();

      root.put("title", title);
      root.put("user", user);
      root.put("unparsed-flags", flags);
      root.put("diff-bytes", byteDiff);
      root.put("diff-url", diffUrl);
      root.put("summary", summary);
      root.put("flags", flagMap);

      return root;
    } else {
      throw new IllegalArgumentException();
    }
  }

  public static void main(String[] args) {
    String[] lines = new String[] { "[[Wikipedia talk:Articles for creation/Lords of War]]  http://en.wikipedia.org/w/index.php?diff=562991653&oldid=562991567 * BBGLordsofWar * (+95) /* Lords of War: Elves versus Lizardmen */]", "[[David Shepard (surgeon)]] M http://en.wikipedia.org/w/index.php?diff=562993463&oldid=562989820 * Jacobsievers * (+115) /* American Revolution (1775�1783) */  Added to note regarding David Shepard's brothers" };

    for (String line : lines) {
      System.out.println(parse(line));
    }
  }
}
