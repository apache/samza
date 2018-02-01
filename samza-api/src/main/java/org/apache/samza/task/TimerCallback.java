package org.apache.samza.task;

/**
 * Created by xiliu on 2/1/18.
 */
public interface TimerCallback<K> {
  void onTimer(K key, MessageCollector collector, TaskCoordinator coordinator);
}
