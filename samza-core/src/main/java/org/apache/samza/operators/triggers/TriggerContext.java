package org.apache.samza.operators.triggers;

public interface TriggerContext {

  public Cancellable scheduleCallback(Runnable listener, long durationMs);

  public Object getWindowKey();

}
