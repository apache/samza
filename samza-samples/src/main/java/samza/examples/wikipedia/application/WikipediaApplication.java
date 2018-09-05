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
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samza.examples.wikipedia.model.WikipediaParser;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


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
    // Messages come from WikipediaConsumer so we know that they don't have a key and don't need to be deserialized.
    graph.setDefaultSerde(new NoOpSerde<>());

    // Inputs
    // Messages come from WikipediaConsumer so we know the type is WikipediaFeedEvent
    MessageStream<WikipediaFeedEvent> wikipediaEvents = graph.getInputStream(WIKIPEDIA_STREAM_ID);
    MessageStream<WikipediaFeedEvent> wiktionaryEvents = graph.getInputStream(WIKTIONARY_STREAM_ID);
    MessageStream<WikipediaFeedEvent> wikiNewsEvents = graph.getInputStream(WIKINEWS_STREAM_ID);

    // Output (also un-keyed)
    OutputStream<WikipediaStatsOutput> wikipediaStats =
        graph.getOutputStream(STATS_STREAM_ID, new JsonSerdeV2<>(WikipediaStatsOutput.class));

    // Merge inputs
    MessageStream<WikipediaFeedEvent> allWikipediaEvents =
        MessageStream.mergeAll(ImmutableList.of(wikipediaEvents, wiktionaryEvents, wikiNewsEvents));

    // Parse, update stats, prepare output, and send
    allWikipediaEvents
        .map(WikipediaParser::parseEvent)
        .window(Windows.tumblingWindow(Duration.ofSeconds(10), WikipediaStats::new,
                new WikipediaStatsAggregator(), WikipediaStats.serde()), "statsWindow")
        .map(this::formatOutput)
        .sendTo(wikipediaStats);
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
  private WikipediaStatsOutput formatOutput(WindowPane<Void, WikipediaStats> statsWindowPane) {
    WikipediaStats stats = statsWindowPane.getMessage();
    return new WikipediaStatsOutput(
        stats.edits, stats.totalEdits, stats.byteDiff, stats.titles.size(), stats.counts);
  }

  /**
   * A few statistics about the incoming messages.
   */
  public static class WikipediaStats {
    // Windowed stats
    int edits = 0;
    int byteDiff = 0;
    Set<String> titles = new HashSet<>();
    Map<String, Integer> counts = new HashMap<>();

    // Total stats
    int totalEdits = 0;

    @Override
    public String toString() {
      return String.format("Stats {edits:%d, byteDiff:%d, titles:%s, counts:%s}", edits, byteDiff, titles, counts);
    }

    static Serde<WikipediaStats> serde() {
      return new WikipediaStatsSerde();
    }

    public static class WikipediaStatsSerde implements Serde<WikipediaStats> {
      @Override
      public WikipediaStats fromBytes(byte[] bytes) {
        try {
          ByteArrayInputStream bias = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = new ObjectInputStream(bias);
          WikipediaStats stats = new WikipediaStats();
          stats.edits = ois.readInt();
          stats.byteDiff = ois.readInt();
          stats.titles = (Set<String>) ois.readObject();
          stats.counts = (Map<String, Integer>) ois.readObject();
          return stats;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public byte[] toBytes(WikipediaStats wikipediaStats) {
        try {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream dos = new ObjectOutputStream(baos);
          dos.writeInt(wikipediaStats.edits);
          dos.writeInt(wikipediaStats.byteDiff);
          dos.writeObject(wikipediaStats.titles);
          dos.writeObject(wikipediaStats.counts);
          return baos.toByteArray();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  static class WikipediaStatsOutput {
    public int edits;
    public int editsAllTime;
    public int bytesAdded;
    public int uniqueTitles;
    public Map<String, Integer> counts;

    public WikipediaStatsOutput(int edits, int editsAllTime, int bytesAdded, int uniqueTitles,
        Map<String, Integer> counts) {
      this.edits = edits;
      this.editsAllTime = editsAllTime;
      this.bytesAdded = bytesAdded;
      this.uniqueTitles = uniqueTitles;
      this.counts = counts;
    }
  }
}

