package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

/**
 * Implementation class for a {@link RepeatingTrigger}
 */
public class RepeatingTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final Trigger underlyingTrigger;

  private TriggerImpl underlyingTriggerImpl;

  private boolean cancelled = false;

  public RepeatingTriggerImpl(RepeatingTrigger<M> repeatingTrigger, TriggerContext tContext, TriggerCallbackHandler handler) {
    super(tContext, handler);
    this.underlyingTrigger = repeatingTrigger.getTrigger();

    if (!cancelled) {
      this.underlyingTriggerImpl = TriggerImpls.createTriggerImpl(underlyingTrigger, context, createNewCallback());
    }
  }

  private TriggerCallbackHandler createNewCallback() {
    return new TriggerCallbackHandler() {
      @Override
      public void onTrigger(TriggerImpl impl, Object storeKey) {
        underlyingTriggerImpl = TriggerImpls.createTriggerImpl(underlyingTrigger, context, createNewCallback());
        handler.onTrigger(RepeatingTriggerImpl.this, storeKey);
      }
    };
  }

  @Override
  public void onMessage(M message) {
    underlyingTriggerImpl.onMessage(message);
  }

  @Override
  public void onCancel() {
    underlyingTriggerImpl.onCancel();
    cancelled = true;
  }
}
