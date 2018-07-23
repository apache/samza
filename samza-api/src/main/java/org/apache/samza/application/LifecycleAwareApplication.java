package org.apache.samza.application;

/**
 * Created by yipan on 7/11/18.
 */
public interface LifecycleAwareApplication<S extends ApplicationSpec> {
  void describe(S appSpec);
  default void beforeStart() {}
  default void afterStart() {}
  default void beforeStop() {}
  default void afterStop() {}
}
