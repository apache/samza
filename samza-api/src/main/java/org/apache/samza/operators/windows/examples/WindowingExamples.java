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

package org.apache.samza.operators.windows.examples;

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.*;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Examples for programming using the {@link Windows} APIs.
 */
public class WindowingExamples {

  private static final Time INTERVAL_MS = Time.milliseconds(1000);
  private static final Time SESSION_GAP_MS = Time.milliseconds(2000);

  private static Integer parseInt(IncomingSystemMessageEnvelope msg) {
    //parse an integer from the message
    return 0;
  }

  private static PercentileMessage compute99Percentile(Collection<IncomingSystemMessageEnvelope> msg) {
    //iterate over values, sort them, and compute percentiles.
    return null;
  }

  private static class PercentileMessage implements MessageEnvelope<String, Integer> {

    @Override
    public String getKey() {
      return null;
    }

    @Override
    public Integer getMessage() {
      return null;
    }

  }

  public static void main(String[] args) {

    MessageStream<IncomingSystemMessageEnvelope> integerStream = null;

    //GLOBAL TUMBLING WINDOWS WITH CUSTOM AGGREGATES: Demonstrate a custom aggregation (MAX) that returns the max element in a {@link MessageStream}
    //Notes: - Aggregations are done with a tumbling window of 1000ms.
    //       - Early results are emitted once every 50 messages, every 4000ms in processing time.
    //       - Late results are emitted only when 20 late messages have accumulated.
    //       - Previously fired window panes are discarded
    //       - Messages have a maximum allowed lateness of 50000ms.

    BiFunction<IncomingSystemMessageEnvelope, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
    MessageStream<WindowOutput<WindowKey<Void>, Integer>> window1 = integerStream.window(Windows.tumblingWindow(INTERVAL_MS, maxAggregator)
        .setTriggers(new TriggersBuilder()
            .withEarlyFiringsAfterCountAtleast(50)
            .withEarlyFiringsEvery(Time.seconds(4))
            .withLateFiringsAfterCountAtleast(20)
            .discardFiredPanes()
            .build()));


    //KEYED SESSION WINDOW WITH PERCENTILE COMPUTATION: Demonstrate percentile computation over a Keyed Session Window on a  {@link MessageStream}
    //Notes: - Aggregations are done with session windows having gap SESSION_GAP_MS seconds.
    //       - Early results are emitted once every 50 messages, every 4000ms in processing time.
    //       - Late results are emitted only when 20 late messages have accumulated.
    //       - Previously fired window panes are accumulated
    //       - Messages have a maximum allowed lateness of 50000ms.
    Function<IncomingSystemMessageEnvelope, String> keyExtractor = null;

    final MessageStream<PercentileMessage> windowedPercentiles = integerStream.window(Windows.keyedSessionWindow(keyExtractor, SESSION_GAP_MS)
          .setTriggers(new TriggersBuilder()
              .withEarlyFiringsAfterCountAtleast(50)
              .withEarlyFiringsEvery(Time.seconds(4))
              .withLateFiringsAfterCountAtleast(20)
              .accumulateFiredPanes()
              .build()))
        .map(dataset -> compute99Percentile(dataset.getMessage()));

    //KEYED TUMBLING WINDOW WITH PERCENTILE COMPUTATION: Demonstrate percentile computation over a Keyed Tumbling Window on a {@link MessageStream}
    final MessageStream<PercentileMessage> tumblingPercentiles = integerStream.window(Windows.keyedTumblingWindow(keyExtractor, INTERVAL_MS)
        .setTriggers(new TriggersBuilder()
            .withEarlyFiringsAfterCountAtleast(50)
            .accumulateFiredPanes()
            .build()))
        .map(dataset -> compute99Percentile(dataset.getMessage()));

    //KEYED TUMBLING WINDOW
    MessageStream<WindowOutput<WindowKey<String>, Collection<IncomingSystemMessageEnvelope>>> tumblingWindow = integerStream.window(Windows.keyedTumblingWindow(keyExtractor, INTERVAL_MS)
        .setTriggers(new TriggersBuilder()
            .withEarlyFiringsAfterCountAtleast(50)
            .discardFiredPanes()
            .build()));


    /*
    //A CUSTOM GLOBAL WINDOW: Demonstrates a window with custom triggering every 500 messages
    final MessageStream<WindowOutput<GlobalWindowInfo, Collection<IncomingSystemMessage>>> customWindow = integerStream.window(Windows.<IncomingSystemMessage>customGlobalWindow().setTriggers(new TriggerSpecBuilder()
        .withEarlyFiringsAfterCountAtleast(500)
        .discardFiredPanes()
        .build()));
   */
  }
}
