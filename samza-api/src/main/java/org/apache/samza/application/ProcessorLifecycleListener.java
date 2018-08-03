package org.apache.samza.application;

import java.io.Serializable;


/**
 * Created by yipan on 8/1/18.
 */
public interface ProcessorLifecycleListener extends Serializable {
  default void beforeStart() {}
  default void afterStart() {}
  default void beforeStop() {}
  default void afterStop() {}
}
