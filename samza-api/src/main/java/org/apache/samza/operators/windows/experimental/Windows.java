package org.apache.samza.operators.windows.experimental;

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.WindowOutput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Windows {

  public static <M extends MessageEnvelope, K, WV> Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedTumblingWindow(Function<M, K> keyFn, long interval, BiFunction<M, WV, WV> aggregateFunction) {

    List<TriggersBuilder.Trigger> defaultTriggers = new ArrayList<>();

    TriggersBuilder.Trigger timeTrigger = new TriggersBuilder.PeriodicTimeTrigger(TriggersBuilder.TriggerType.DEFAULT, interval,
        TriggersBuilder.TimeCharacteristic.PROCESSING_TIME);
    defaultTriggers.add(timeTrigger);

    return new WindowDefinition<M, K, WV>(keyFn, aggregateFunction, null, defaultTriggers);
  }

  public static <M extends MessageEnvelope, K> Window<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>> keyedTumblingWindow(Function<M, K> keyFn, long interval) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedTumblingWindow(keyFn, interval, aggregator);
  }

  public static <M extends MessageEnvelope, WV> Window<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>> tumblingWindow(long interval, BiFunction<M, WV, WV> aggregateFunction) {

    List<TriggersBuilder.Trigger> defaultTriggers = new ArrayList<>();

    TriggersBuilder.Trigger timeTrigger = new TriggersBuilder.PeriodicTimeTrigger(TriggersBuilder.TriggerType.DEFAULT, interval,
        TriggersBuilder.TimeCharacteristic.PROCESSING_TIME);
    defaultTriggers.add(timeTrigger);

    return new WindowDefinition<M, Void, WV>(null, aggregateFunction, null, defaultTriggers);
  }

  public static <M extends MessageEnvelope > Window<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> tumblingWindow(long interval) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return tumblingWindow(interval, aggregator);
  }

  public static <M extends MessageEnvelope, WV> Window<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>> sessionWindow(long sessionGap, BiFunction<M, WV, WV> aggregateFunction) {

    List<TriggersBuilder.Trigger> defaultTriggers = new ArrayList<>();

    TriggersBuilder.Trigger timeTrigger = new TriggersBuilder.TimeSinceLastMessageTrigger(TriggersBuilder.TriggerType.DEFAULT, sessionGap,
      TriggersBuilder.TimeCharacteristic.PROCESSING_TIME);
    defaultTriggers.add(timeTrigger);

    return new WindowDefinition<M, Void, WV>(null, aggregateFunction, null, defaultTriggers);
  }

  public static <M extends MessageEnvelope > Window<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> sessionWindow(long gap) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return sessionWindow(gap, aggregator);
  }

  public static <M extends MessageEnvelope, K, WV> Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedSessionWindow(Function<M, K> keyFn, long sessionGap, BiFunction<M, WV, WV> aggregateFunction) {
    List<TriggersBuilder.Trigger> defaultTriggers = new ArrayList<>();

    TriggersBuilder.Trigger timeTrigger = new TriggersBuilder.TimeSinceLastMessageTrigger(TriggersBuilder.TriggerType.DEFAULT, sessionGap,
      TriggersBuilder.TimeCharacteristic.PROCESSING_TIME);
    defaultTriggers.add(timeTrigger);

    return new WindowDefinition<M, K, WV>(keyFn, aggregateFunction, null, defaultTriggers);
  }

  public static <M extends MessageEnvelope, K> Window<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>> keyedSessionWindow(Function<M, K> keyFn, long sessionGap) {

    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedSessionWindow(keyFn, sessionGap, aggregator);
  }



  public static <M extends MessageEnvelope, WV> Window<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>> globalWindow(BiFunction<M, WV, WV> aggregateFunction) {
    return new WindowDefinition<M, Void, WV>(null, aggregateFunction, null, null);
  }

  public static <M extends MessageEnvelope > Window<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> globalWindow() {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return globalWindow(aggregator);
  }

  public static <M extends MessageEnvelope, K, WV> Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedGlobalWindow(Function<M, K> keyFn, BiFunction<M, WV, WV> aggregateFunction) {
    return new WindowDefinition<M, K, WV>(keyFn, aggregateFunction, null, null);
  }

  public static <M extends MessageEnvelope, K> Window<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>> keyedGlobalWindow(Function<M, K> keyFn) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedGlobalWindow(keyFn, aggregator);
  }
}
