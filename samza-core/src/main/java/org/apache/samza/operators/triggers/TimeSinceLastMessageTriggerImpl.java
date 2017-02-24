package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

/**
 * Created by jvenkatr on 1/31/17.
 */
public class TimeSinceLastMessageTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final TimeSinceLastMessageTrigger trigger;
  private final long durationMs;
  private long callbackTime = Integer.MIN_VALUE;
  private Cancellable latestFuture = null;

  public TimeSinceLastMessageTriggerImpl(TimeSinceLastMessageTrigger<M> trigger, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.trigger = trigger;
    this.durationMs = trigger.getDuration().toMillis();
  }


  @Override
  public void onMessage(M message) {

    long currTime = System.currentTimeMillis();

    if (currTime < callbackTime && latestFuture != null) {
      latestFuture.cancel();
    }

    callbackTime = currTime + durationMs;
    Runnable runnable = () -> {
      handler.onTrigger(this, context);
    };

    latestFuture = context.scheduleCallback(runnable, callbackTime);
    }
  }
