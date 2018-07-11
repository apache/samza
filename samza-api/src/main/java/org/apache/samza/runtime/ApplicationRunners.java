package org.apache.samza.runtime;

import java.lang.reflect.Constructor;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.runtime.internal.ApplicationRunner;


/**
 * Created by yipan on 7/10/18.
 */
public class ApplicationRunners {

  static class AppRunnerConfig {
    private static final String APP_RUNNER_CFG = "app.runner.class";
    private static final String DEFAULT_APP_RUNNER = "org.apache.samza.runtime.RemoteApplicationRunner";

    private final Config config;

    AppRunnerConfig(Config config) {
      this.config = config;
    }

    String getAppRunnerClass() {
      return this.config.getOrDefault(APP_RUNNER_CFG, DEFAULT_APP_RUNNER);
    }

    static String getAppRunnerCfg() {
      return APP_RUNNER_CFG;
    }

  }

  /**
   * Static method to load the {@link ApplicationRunner}
   *
   * @param config  configuration passed in to initialize the Samza processes
   * @return  the configure-driven {@link ApplicationRunner} to run the user-defined stream applications
   */
  public static ApplicationRunner fromConfig(Config config) {
    AppRunnerConfig appRunnerCfg = new AppRunnerConfig(config);
    try {
      Class<?> runnerClass = Class.forName(appRunnerCfg.getAppRunnerClass());
      if (ApplicationRunner.class.isAssignableFrom(runnerClass)) {
        Constructor<?> constructor = runnerClass.getConstructor(Config.class); // *sigh*
        return (ApplicationRunner) constructor.newInstance(config);
      }
    } catch (Exception e) {
      throw new ConfigException(String.format("Problem in loading ApplicationRunner class %s",
          appRunnerCfg.getAppRunnerClass()), e);
    }
    throw new ConfigException(String.format(
        "Class %s does not extend ApplicationRunner properly",
        appRunnerCfg.getAppRunnerClass()));
  }

}
