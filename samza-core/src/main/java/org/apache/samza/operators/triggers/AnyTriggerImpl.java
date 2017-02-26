package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of an {@link AnyTrigger}
 *
 * <p>
 */
public class AnyTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final List<Trigger> triggerList;

  private final Map<TriggerImpl, Boolean> triggerImpls = new HashMap<>();

  public AnyTriggerImpl(AnyTrigger<M> anyTrigger, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.triggerList = anyTrigger.getTriggers();

    for (Trigger trigger : triggerList) {
      triggerImpls.put(TriggerImpls.createTriggerImpl(trigger, context, createHandler()), false);
    }
  }

  private TriggerCallbackHandler createHandler() {
    return new TriggerCallbackHandler() {
      @Override
      public void onTrigger(TriggerImpl trigger, Object storeKey) {
        handler.onTrigger(AnyTriggerImpl.this, storeKey);
        onCancel();
      }
    };
  }

  @Override
  public void onMessage(M message) {
    for (TriggerImpl triggerImpl : triggerImpls.keySet()) {
      triggerImpl.onMessage(message);
    }
  }

  public void onCancel() {
    for(Iterator<Map.Entry<TriggerImpl, Boolean>> it = triggerImpls.entrySet().iterator(); it.hasNext(); ) {
      TriggerImpl impl = it.next().getKey();
      impl.onCancel();
      it.remove();
    }
  }
}
