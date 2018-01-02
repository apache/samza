package org.apache.samza.application;

import org.apache.samza.config.Config;


/**
 * Created by yipan on 12/22/17.
 */
public interface UserDefinedStreamApplication {
  void init(Config config, StreamApplication application) throws Exception;
}
