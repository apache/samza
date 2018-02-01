package org.apache.samza.operators.functions;

import java.util.Collection;

/**
 * Created by xiliu on 2/1/18.
 */
public interface TimerFunction<K, OM> {

  interface TimerRegistry<T> {
    void register(T key, long delay);
    void delete(T key);
  }

  void initTimers(TimerRegistry<K> timerRegistry);

  Collection<OM> onTimer(K key);
}
