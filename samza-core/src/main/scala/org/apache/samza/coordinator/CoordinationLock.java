package org.apache.samza.coordinator;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public interface CoordinationLock {
  void lock(TimeUnit timeUnit, long timeout) throws TimeoutException;
  void unlock();
  void close();
}
