package org.apache.samza.operators.triggers;

/**
 * Provides convenience methods for use by {@link TriggerImpl}s. This includes scheduling and cancelling
 * callbacks, accessing window keys etc.
 */
public interface TriggerContext {

  /**
   * Schedule the provided runnable for execution at the specified duration.
   * @param runnable the provided runnable to schedule.
   * @param callbackTimeMs time at which the runnable must be scheduled for execution.
   * @return a {@link Cancellable} instance which can be used to cancel the execution of this runnable.
   */
  public Cancellable scheduleCallback(Runnable runnable, long callbackTimeMs);

  public Object getWindowKey();
}
