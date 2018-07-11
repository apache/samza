package org.apache.samza.application.internal;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;


/**
 * Created by yipan on 7/10/18.
 */
public abstract class ApplicationSpec {
  final Config config;

  protected ApplicationSpec(Config config) {
    this.config = config;
  }

  public static class AppConfig extends MapConfig {

    public static final String APP_NAME = "app.name";
    public static final String APP_ID = "app.id";
    public static final String APP_CLASS = "app.class";

    public static final String JOB_NAME = "job.name";
    public static final String JOB_ID = "job.id";

    public AppConfig(Config config) {
      super(config);
    }

    public String getAppName() {
      return get(APP_NAME, get(JOB_NAME));
    }

    public String getAppId() {
      return get(APP_ID, get(JOB_ID, "1"));
    }

    public String getAppClass() {
      return get(APP_CLASS, null);
    }

    /**
     * Returns full application id
     *
     * @return full app id
     */
    public String getGlobalAppId() {
      return String.format("app-%s-%s", getAppName(), getAppId());
    }

  }

  public String getGlobalAppId() {
    return new AppConfig(config).getGlobalAppId();
  }

}