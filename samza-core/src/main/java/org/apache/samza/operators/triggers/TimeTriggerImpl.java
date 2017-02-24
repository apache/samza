package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;


/**
 * Created by jvenkatr on 2/8/17.
 */
public class TimeTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final TimeTrigger<M> trigger;
  private Cancellable latestFuture;

  public TimeTriggerImpl (TimeTrigger<M> trigger, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.trigger = trigger;
  }

  public void onMessage(M message) {

    final long now = System.currentTimeMillis();
    long triggerDurationMs = trigger.getDuration().toMillis();
    Long callbackTime = (now - now % triggerDurationMs) + triggerDurationMs;

    if (latestFuture == null) {
      latestFuture =  context.scheduleCallback(() -> {
        handler.onTrigger(TimeTriggerImpl.this, context.getWindowKey());
      }, callbackTime);
    }
   }
  }
