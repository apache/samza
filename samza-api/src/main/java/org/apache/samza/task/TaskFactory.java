package org.apache.samza.task;

import org.apache.samza.annotation.InterfaceStability;


/**
 * Created by yipan on 7/10/18.
 */
@InterfaceStability.Stable
public interface TaskFactory<T> {
  T createInstance();
}
