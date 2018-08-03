package org.apache.samza.application;

/**
 * Created by yipan on 7/11/18.
 */
public interface ApplicationBase<S extends ApplicationSpec> {
  void describe(S appSpec);
}
