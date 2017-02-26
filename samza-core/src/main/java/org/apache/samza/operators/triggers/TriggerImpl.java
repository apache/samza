package org.apache.samza.operators.triggers;

/**
 * Implementation class for a {@link Trigger}. A {@link TriggerImpl} is used with a {@link TriggerCallbackHandler}
 * which is invoked when the trigger fires.
 *
 * <p> When MessageEnvelopes arrive in the {@code WindowOperatorImpl}, they are assigned to one or more windows. An
 * instance of a {@link TriggerImpl} is created corresponding to each {@link Trigger} configured for a window. For every
 * MessageEnvelope added to the window, the {@code WindowOperatorImpl} invokes the {@link #onMessage} on its corresponding
 * {@link TriggerImpl}s. A {@link TriggerImpl} instance is scoped to a window and its firing determines when results for
 * its window are emitted.
 *
 * {@link TriggerImpl}s can use the {@link TriggerContext} to schedule and cancel callbacks (for example, time-based triggers).
 *
 * <p> State management: The state maintained by {@link TriggerImpl}s is not durable across re-starts and is transient.
 * New instances of {@link TriggerImpl} are created on a re-start.
 *
 */
public abstract class TriggerImpl<M> {

  protected final TriggerCallbackHandler handler;

  protected final TriggerContext context;

  public TriggerImpl(TriggerContext context, TriggerCallbackHandler handler) {
    this.handler = handler;
    this.context = context;
  }

  /**
   * Invoked when a MessageEnvelope added to the window corresponding to this {@link TriggerImpl}.
   * @param message the incoming MessageEnvelope
   */
  public abstract void onMessage(M message);

  /**
   * Invoked when the execution of this {@link TriggerImpl} is canceled by an unstream {@link TriggerImpl}.
   */
  public abstract void onCancel();

  public interface TriggerCallbackHandler {
    public void onTrigger(TriggerImpl impl, Object storeKey);
  }
}
