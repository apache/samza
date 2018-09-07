package org.apache.samza.scheduling;

import org.apache.samza.task.SystemTimerScheduler;
import org.apache.samza.task.TimerCallback;


/**
 * Delegates to {@link SystemTimerScheduler}. This is useful because it provides a write-only interface for user-facing
 * purposes.
 */
public class SchedulerImpl implements Scheduler {
  private final SystemTimerScheduler systemTimerScheduler;

  public SchedulerImpl(SystemTimerScheduler systemTimerScheduler) {
    this.systemTimerScheduler = systemTimerScheduler;
  }

  @Override
  public <K> void scheduleCallback(K key, long timestamp, TimerCallback<K> callback) {
    this.systemTimerScheduler.setTimer(key, timestamp, callback);
  }

  @Override
  public <K> void deleteCallback(K key) {
    this.systemTimerScheduler.deleteTimer(key);
  }
}
