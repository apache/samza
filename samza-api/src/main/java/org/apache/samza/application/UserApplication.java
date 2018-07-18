package org.apache.samza.application;

import org.apache.samza.application.internal.ApplicationBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.internal.ApplicationSpec;


/**
 * Created by yipan on 7/11/18.
 */
public interface UserApplication<T extends UserApplication, B extends ApplicationBuilder<T>, S extends ApplicationSpec<T>> {
  void init(B appBuilder, Config config);
  default void beforeStart(S appSpec) {}
  default void afterStart(S appSpec) {}
  default void beforeStop(S appSpec) {}
  default void afterStop(S appSpec) {}
}
