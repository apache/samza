package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

/**
 * Created by jvenkatr on 1/31/17.
 */
public class TriggerImpls {

  public static <M extends MessageEnvelope> TriggerImpl<M> createTriggerImpl(Trigger<M> trigger, TriggerContext context, TriggerImpl.TriggerCallbackHandler handler) {

    if (trigger instanceof CountTrigger) {
      return new CountTriggerImpl<>((CountTrigger)trigger, context, handler);
    } else if (trigger instanceof RepeatingTrigger) {
      return new RepeatingTriggerImpl<>((RepeatingTrigger) trigger, context, handler);
    } else if (trigger instanceof AnyTrigger) {
      return new AnyTriggerImpl<>((AnyTrigger) trigger, context, handler);
    } else if (trigger instanceof TimeSinceLastMessageTrigger) {
      return new TimeSinceLastMessageTriggerImpl<>((TimeSinceLastMessageTrigger)trigger, context, handler);
    } else if (trigger instanceof TimeTrigger) {
      return new TimeTriggerImpl((TimeTrigger)trigger, context, handler);
    }
    return null;
  }
}
