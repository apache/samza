package org.apache.samza.operators.windows.experimental;

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.WindowOutput;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *  The base class of all types of {@link Window}s. Sub-classes can specify the default triggering semantics
 *  for the {@link Window}, semantics for emission of early or late results and whether to accumulate or discard
 *  previous results.
 */

public class WindowDefinition<M extends MessageEnvelope, K, WV> implements Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> {

  /**
   * Defines the default triggering semantics for the {@link Window}.
   */
  private final List<TriggersBuilder.Trigger> defaultFirings;


  /**
   * Defines the triggering semantics for emission of early or late results.
   */
  private List<TriggersBuilder.Trigger> earlyFirings;
  private List<TriggersBuilder.Trigger> lateFirings;

  /**
   * Defines the fold function that is applied each time a {@link MessageEnvelope} is added to the window.
   */
  private final BiFunction<M, WV, WV> aggregator;

  /*
   * Defines the function that extracts the event time from a {@link MessageEnvelope}
   */
  private final Function<M, Long> eventTimeExtractor;

  /*
   * Defines the function that extracts the key from a {@link MessageEnvelope}
   */
  private final Function<M, K> keyExtractor;


  public WindowDefinition(Function<M, K> keyExtractor, BiFunction<M, WV, WV> aggregator, Function<M, Long> eventTimeExtractor, List<TriggersBuilder.Trigger> defaultFirings) {
    this.aggregator = aggregator;
    this.eventTimeExtractor = eventTimeExtractor;
    this.keyExtractor = keyExtractor;
    this.defaultFirings = defaultFirings;
  }

  @Override
  public Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> setTriggers(TriggersBuilder.Triggers wndTrigger) {
    this.earlyFirings = wndTrigger.getEarlyTriggers();
    this.lateFirings = wndTrigger.getLateTriggers();
    return this;
  }

  public List<TriggersBuilder.Trigger> getDefaultFirings() {
    return defaultFirings;
  }

  public List<TriggersBuilder.Trigger> getEarlyFirings() {
    return earlyFirings;
  }

  public List<TriggersBuilder.Trigger> getLateFirings() {
    return lateFirings;
  }

  public BiFunction<M, WV, WV> getAggregator() {
    return aggregator;
  }

  public Function<M, Long> getEventTimeExtractor() {
    return eventTimeExtractor;
  }

  public Function<M, K> getKeyExtractor() {
    return keyExtractor;
  }
}
