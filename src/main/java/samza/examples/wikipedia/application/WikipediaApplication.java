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

package samza.examples.wikipedia.application;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samza.examples.wikipedia.model.WikipediaParser;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;


/**
 * This {@link StreamApplication} demonstrates the Samza fluent API by performing the same operations as
 * {@link samza.examples.wikipedia.task.WikipediaFeedStreamTask},
 * {@link samza.examples.wikipedia.task.WikipediaParserStreamTask}, and
 * {@link samza.examples.wikipedia.task.WikipediaStatsStreamTask} in one expression.
 *
 * The only functional difference is the lack of "wikipedia-raw" and "wikipedia-edits"
 * streams to connect the operators, as they are not needed with the fluent API.
 *
 * The application processes Wikipedia events in the following steps:
 * <ul>
 *   <li>Merge wikipedia, wiktionary, and wikinews events into one stream</li>
 *   <li>Parse each event to a more structured format</li>
 *   <li>Aggregate some stats over a 10s window</li>
 *   <li>Format each window output for public consumption</li>
 *   <li>Send the window output to Kafka</li>
 * </ul>
 *
 * All of this application logic is defined in the {@link #init(StreamGraph, Config)} method, which
 * is invoked by the framework to load the application.
 */
public class WikipediaApplication implements StreamApplication {
  private static final Logger log = LoggerFactory.getLogger(WikipediaApplication.class);

  // Inputs
  private static final String WIKIPEDIA_STREAM_ID = "en-wikipedia";
  private static final String WIKTIONARY_STREAM_ID = "en-wiktionary";
  private static final String WIKINEWS_STREAM_ID = "en-wikinews";

  // Outputs
  private static final String STATS_STREAM_ID = "wikipedia-stats";

  // Stores
  private static final String STATS_STORE_NAME = "wikipedia-stats";

  // Metrics
  private static final String EDIT_COUNT_KEY = "count-edits-all-time";

  @Override
  public void init(StreamGraph graph, Config config) {
    // Inputs
    // Messages come from WikipediaConsumer so we know the type is WikipediaFeedEvent
    // They are un-keyed, so the 'k' parameter to the msgBuilder is not used
    MessageStream<WikipediaFeedEvent> wikipediaEvents = graph.getInputStream(WIKIPEDIA_STREAM_ID, (k, v) -> (WikipediaFeedEvent) v);
    MessageStream<WikipediaFeedEvent> wiktionaryEvents = graph.getInputStream(WIKTIONARY_STREAM_ID, (k, v) -> (WikipediaFeedEvent) v);
    MessageStream<WikipediaFeedEvent> wikiNewsEvents = graph.getInputStream(WIKINEWS_STREAM_ID, (k, v) -> (WikipediaFeedEvent) v);

    // Output (also un-keyed, so no keyExtractor)
    OutputStream<Void, Map<String, Integer>, Map<String, Integer>> wikipediaStats = graph.getOutputStream(STATS_STREAM_ID, m -> null, m -> m);

    // Merge inputs
    MessageStream<WikipediaFeedEvent> allWikipediaEvents = MessageStream.mergeAll(ImmutableList.of(wikipediaEvents, wiktionaryEvents, wikiNewsEvents));

    // Parse, update stats, prepare output, and send
    allWikipediaEvents.map(WikipediaParser::parseEvent)
        .window(Windows.tumblingWindow(Duration.ofSeconds(10), WikipediaStats::new, new WikipediaStatsAggregator()))
        .map(this::formatOutput)
        .sendTo(wikipediaStats);
  }

  /**
   * A few statistics about the incoming messages.
   */
  private static class WikipediaStats {
    // Windowed stats
    int edits = 0;
    int byteDiff = 0;
    Set<String> titles = new HashSet<String>();
    Map<String, Integer> counts = new HashMap<String, Integer>();

    // Total stats
    int totalEdits = 0;

    @Override
    public String toString() {
      return String.format("Stats {edits:%d, byteDiff:%d, titles:%s, counts:%s}", edits, byteDiff, titles, counts);
    }
  }

  /**
   * Updates the windowed and total stats based on each "edit" event.
   *
   * Uses a KeyValueStore to persist a total edit count across restarts.
   */
  private class WikipediaStatsAggregator implements FoldLeftFunction<Map<String, Object>, WikipediaStats> {

    private KeyValueStore<String, Integer> store;

    // Example metric. Running counter of the number of repeat edits of the same title within a single window.
    private Counter repeatEdits;

    /**
     * {@inheritDoc}
     * Override {@link org.apache.samza.operators.functions.InitableFunction#init(Config, TaskContext)} to
     * get a KeyValueStore for persistence and the MetricsRegistry for metrics.
     */
    @Override
    public void init(Config config, TaskContext context) {
      store = (KeyValueStore<String, Integer>) context.getStore(STATS_STORE_NAME);
      repeatEdits = context.getMetricsRegistry().newCounter("edit-counters", "repeat-edits");
    }

    @Override
    public WikipediaStats apply(Map<String, Object> edit, WikipediaStats stats) {

      // Update persisted total
      Integer editsAllTime = store.get(EDIT_COUNT_KEY);
      if (editsAllTime == null) editsAllTime = 0;
      editsAllTime++;
      store.put(EDIT_COUNT_KEY, editsAllTime);

      // Update window stats
      stats.edits++;
      stats.totalEdits = editsAllTime;
      stats.byteDiff += (Integer) edit.get("diff-bytes");
      boolean newTitle = stats.titles.add((String) edit.get("title"));

      Map<String, Boolean> flags = (Map<String, Boolean>) edit.get("flags");
      for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
        if (Boolean.TRUE.equals(flag.getValue())) {
          stats.counts.compute(flag.getKey(), (k, v) -> v == null ? 0 : v + 1);
        }
      }

      if (!newTitle) {
        repeatEdits.inc();
        log.info("Frequent edits for title: {}", edit.get("title"));
      }
      return stats;
    }
  }

  /**
   * Format the stats for output to Kafka.
   */
  private Map<String, Integer> formatOutput(WindowPane<Void, WikipediaStats> statsWindowPane) {

    WikipediaStats stats = statsWindowPane.getMessage();

    Map<String, Integer> counts = new HashMap<String, Integer>(stats.counts);
    counts.put("edits", stats.edits);
    counts.put("edits-all-time", stats.totalEdits);
    counts.put("bytes-added", stats.byteDiff);
    counts.put("unique-titles", stats.titles.size());

    return counts;
  }
}

