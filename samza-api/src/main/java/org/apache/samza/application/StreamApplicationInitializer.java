package org.apache.samza.application;

/**
 * Created by yipan on 12/22/17.
 */
public interface StreamApplicationInitializer {
  void init(StreamApplication application) throws Exception;
}
