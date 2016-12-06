package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

/**
 * A {@link Trigger} that wraps and repeats its underlying trigger forever.
 * <pre> {@code
 *  Trigger repeatingCount = RepeatTrigger.forever(Triggers.count(5));
 *  }
 */

class RepeatTrigger<M extends MessageEnvelope, K, V> extends Trigger<M, K, V> {

  private final Trigger<M, K, V> trigger;

  RepeatTrigger(Trigger<M, K, V> trigger) {
    this.trigger = trigger;
  }

  public static <M extends MessageEnvelope, K, V> Trigger<M, K, V> forever(Trigger<M, K, V> trigger) {
    return new RepeatTrigger<>(trigger);
  }
}
