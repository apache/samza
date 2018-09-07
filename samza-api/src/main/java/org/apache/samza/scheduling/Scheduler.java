package org.apache.samza.scheduling;

import org.apache.samza.task.TimerCallback;


/**
 * Provides a way for applications to register some logic to be executed at a future time.
 */
public interface Scheduler {
  /**
   * Register a keyed callback of {@link TimerCallback} in this task.
   * The callback will be invoked exclusively with any other operations for this task, e.g. processing, windowing, and
   * commit.
   * @param key callback key
   * @param timestamp epoch time when the callback will be fired, in milliseconds
   * @param callback callback to run
   * @param <K> type of the key
   */
  <K> void scheduleCallback(K key, long timestamp, TimerCallback<K> callback);

  /**
   * Delete the keyed callback in this task.
   * Deletion only happens if the callback hasn't been fired. Otherwise it will not interrupt.
   * @param key callback key
   * @param <K> type of the key
   */
  <K> void deleteCallback(K key);
}
