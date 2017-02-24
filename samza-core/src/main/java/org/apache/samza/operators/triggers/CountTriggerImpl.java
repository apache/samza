package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

/**
 *
 */
public class CountTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final long triggerCount;
  private long currentCount;

  public CountTriggerImpl(CountTrigger<M> triggerCount, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.triggerCount = triggerCount.getCount();
    this.currentCount = 0;
  }

  public void onMessage(M message) {
    currentCount++;
    if (currentCount == triggerCount) {
      handler.onTrigger(this, context.getWindowKey());
    }
  }
}
