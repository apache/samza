package org.apache.samza.operators.triggers;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * An implementation of the {@link TriggerContext} that uses a {@link Timer} to schedule triggers.
 */
public class TestTriggerContext implements TriggerContext {

  private final Timer timer = new Timer();
  Map<Cancellable, TimerTask> map = new HashMap<>();

  @Override
  public Cancellable scheduleCallback(Runnable runnable, long callbackTimeMs) {
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        runnable.run();
      }
    };

    Cancellable impl = new CancellableImpl();
    map.put(impl, task);
    Long delay = callbackTimeMs - System.currentTimeMillis();
    if (delay < 0) {
      delay = 0l;
    }

    timer.schedule(task, delay);
    return impl;
  }

  @Override
  public Object getWindowKey() {
    return null;
  }

  private class CancellableImpl implements Cancellable {
    @Override
    public boolean cancel() {
      return map.get(this).cancel();
    }
  }
}
