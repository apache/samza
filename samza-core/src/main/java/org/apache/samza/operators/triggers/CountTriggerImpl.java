package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

/**
 * Implementation class for a {@link CountTrigger}
 */
public class CountTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final long triggerCount;
  private long currentCount;
  private boolean cancelled = false;

  public CountTriggerImpl(CountTrigger<M> triggerCount, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.triggerCount = triggerCount.getCount();
    this.currentCount = 0;
  }

  public void onMessage(M message) {
    currentCount++;
    if (currentCount == triggerCount && !cancelled) {
      handler.onTrigger(this, context.getWindowKey());
    }
  }

  @Override
  public void onCancel() {
    cancelled = true;
  }
}
