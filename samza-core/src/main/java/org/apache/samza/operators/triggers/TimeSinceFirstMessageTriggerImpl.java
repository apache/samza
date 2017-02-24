package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

public class TimeSinceFirstMessageTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {
  private final TimeSinceFirstMessageTrigger<M> trigger;
  private Cancellable latestFuture;

  public TimeSinceFirstMessageTriggerImpl(TimeSinceFirstMessageTrigger<M> trigger, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.trigger = trigger;
  }

  public void onMessage(M message) {
    if (latestFuture == null) {
      final long now = System.currentTimeMillis();
      long triggerDurationMs = trigger.getDuration().toMillis();
      Long callbackTime = now + triggerDurationMs;

      latestFuture =  context.scheduleCallback(() -> {
        handler.onTrigger(TimeSinceFirstMessageTriggerImpl.this, context.getWindowKey());
      }, callbackTime);
    }
  }

  @Override
  public void onCancel() {
    latestFuture.cancel();
  }
}
