package org.apache.samza.context;

import org.apache.samza.config.Config;


public class JobContextImpl implements JobContext {
  private final Config config;

  public JobContextImpl(Config config) {
    this.config = config;
  }

  @Override
  public Config getConfig() {
    return this.config;
  }
}
