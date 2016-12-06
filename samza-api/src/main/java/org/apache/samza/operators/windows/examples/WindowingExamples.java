package org.apache.samza.operators.windows.examples;

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.experimental.TriggersBuilder;
import org.apache.samza.operators.windows.experimental.WindowKey;
import org.apache.samza.operators.windows.experimental.Windows;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Examples for programming using the {@link org.apache.samza.operators.windows.experimental.Windows} APIs.
 */
public class WindowingExamples {

  private static final long INTERVAL_MS = 1000;
  private static final long SESSION_GAP_MS = 2000;

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
    MessageStream<WindowOutput<WindowKey<Void>, Integer>> window1 = integerStream.window(Windows.tumblingWindow(10000, maxAggregator)
        .setTriggers(new TriggersBuilder()
            .withEarlyFiringsAfterCountAtleast(50)
            .withEarlyFiringsEvery(4000)
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
              .withEarlyFiringsEvery(4000)
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
    final MessageStream<WindowOutput<GlobalWindowKey, Collection<IncomingSystemMessage>>> customWindow = integerStream.window(Windows.<IncomingSystemMessage>customGlobalWindow().setTriggers(new TriggerSpecBuilder()
        .withEarlyFiringsAfterCountAtleast(500)
        .discardFiredPanes()
        .build()));
   */
  }
}
