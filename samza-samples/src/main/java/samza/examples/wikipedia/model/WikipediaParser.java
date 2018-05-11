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

package samza.examples.wikipedia.model;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import samza.examples.wikipedia.system.WikipediaFeed;


public class WikipediaParser {
  public static Map<String, Object> parseEvent(WikipediaFeed.WikipediaFeedEvent wikipediaFeedEvent) {
    Map<String, Object> parsedJsonObject = null;
    try {
      parsedJsonObject = WikipediaParser.parseLine(wikipediaFeedEvent.getRawEvent());

      parsedJsonObject.put("channel", wikipediaFeedEvent.getChannel());
      parsedJsonObject.put("source", wikipediaFeedEvent.getSource());
      parsedJsonObject.put("time", wikipediaFeedEvent.getTime());
    } catch (Exception e) {
      System.err.println("Unable to parse line: " + wikipediaFeedEvent);
    }
    return parsedJsonObject;
  }

  public static Map<String, Object> parseLine(String line) {
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
}
