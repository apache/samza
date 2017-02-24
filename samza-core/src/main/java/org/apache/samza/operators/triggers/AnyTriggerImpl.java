package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class AnyTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final List<Trigger> triggerList;

  private final List<TriggerImpl> triggerImpls = new LinkedList<>();

  public AnyTriggerImpl(AnyTrigger<M> anyTrigger, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.triggerList = anyTrigger.getTriggers();

    for (Trigger trigger : triggerList) {
      triggerImpls.add(TriggerImpls.createTriggerImpl(trigger, context, createHandler()));
    }
  }

  public TriggerCallbackHandler createHandler() {
    return new TriggerCallbackHandler() {
      @Override
      public void onTrigger(TriggerImpl trigger, Object storeKey) {
        handler.onTrigger(AnyTriggerImpl.this, storeKey);
      }
    };
  }

  public void onMessage(M message) {
    for (TriggerImpl triggerImpl : triggerImpls) {
      triggerImpl.onMessage(message);
    }
  }
}
