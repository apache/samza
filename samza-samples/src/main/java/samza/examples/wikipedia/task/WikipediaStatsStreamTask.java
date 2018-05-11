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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

public class WikipediaStatsStreamTask implements StreamTask, InitableTask, WindowableTask {
  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "wikipedia-stats");

  private int edits = 0;
  private int byteDiff = 0;
  private Set<String> titles = new HashSet<String>();
  private Map<String, Integer> counts = new HashMap<String, Integer>();
  private KeyValueStore<String, Integer> store;

  // Example metric. Running counter of the number of repeat edits of the same title within a single window.
  private Counter repeatEdits;

  public void init(Config config, TaskContext context) {
    this.store = (KeyValueStore<String, Integer>) context.getStore("wikipedia-stats");
    this.repeatEdits = context.getMetricsRegistry().newCounter("edit-counters", "repeat-edits");
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    Map<String, Object> edit = (Map<String, Object>) envelope.getMessage();
    Map<String, Boolean> flags = (Map<String, Boolean>) edit.get("flags");

    Integer editsAllTime = store.get("count-edits-all-time");
    if (editsAllTime == null) editsAllTime = 0;
    store.put("count-edits-all-time", editsAllTime + 1);

    edits += 1;
    byteDiff += (Integer) edit.get("diff-bytes");
    boolean newTitle = titles.add((String) edit.get("title"));

    for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
      if (Boolean.TRUE.equals(flag.getValue())) {
        counts.compute(flag.getKey(), (k, v) -> v == null ? 0 : v + 1);
      }
    }

    if (!newTitle) {
      repeatEdits.inc();
    }
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    counts.put("edits", edits);
    counts.put("bytes-added", byteDiff);
    counts.put("unique-titles", titles.size());
    counts.put("edits-all-time", store.get("count-edits-all-time"));

    collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, counts));

    // Reset counts after windowing.
    edits = 0;
    byteDiff = 0;
    titles = new HashSet<String>();
    counts = new HashMap<String, Integer>();
  }
}
