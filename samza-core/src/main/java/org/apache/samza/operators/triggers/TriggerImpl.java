package org.apache.samza.operators.triggers;

/**
 *
 */
public abstract class TriggerImpl<M> {

  protected final TriggerCallbackHandler handler;

  protected final TriggerContext context;

  public TriggerImpl(TriggerContext context, TriggerCallbackHandler handler) {
    this.handler = handler;
    this.context = context;
  }

  public abstract void onMessage(M message);

  public interface TriggerCallbackHandler {
    public void onTrigger(TriggerImpl impl, Object storeKey);
  }
}
