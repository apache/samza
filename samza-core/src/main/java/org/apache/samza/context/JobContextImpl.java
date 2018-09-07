package org.apache.samza.context;

import org.apache.samza.config.Config;


public class JobContextImpl implements JobContext {
  private final Config _config;

  public JobContextImpl(Config config) {
    _config = config;
  }

  @Override
  public Config getConfig() {
    return _config;
  }
}
