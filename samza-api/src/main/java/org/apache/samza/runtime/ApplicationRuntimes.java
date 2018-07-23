package org.apache.samza.runtime;

import java.time.Duration;
import java.util.Map;
import org.apache.samza.application.ApplicationSpec;
import org.apache.samza.application.LifecycleAwareApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.internal.StreamAppSpecImpl;
import org.apache.samza.application.internal.TaskAppSpecImpl;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.apache.samza.runtime.internal.ApplicationRunners;


/**
 * Created by yipan on 7/11/18.
 */
public class ApplicationRuntimes {

  private ApplicationRuntimes() {

  }

  public static final ApplicationRuntime getApplicationRuntime(LifecycleAwareApplication userApp, Config config) {
    if (userApp instanceof StreamApplication) {
      return new AppRuntimeImpl(new StreamAppSpecImpl((StreamApplication) userApp, config));
    }
    if (userApp instanceof TaskApplication) {
      return new AppRuntimeImpl(new TaskAppSpecImpl((TaskApplication) userApp, config));
    }
    throw new IllegalArgumentException(String.format("User application instance has to be either StreamApplicationFactory or TaskApplicationFactory. "
        + "Invalid userApp class %s.", userApp.getClass().getName()));
  }

  private static class AppRuntimeImpl implements ApplicationRuntime {
    private final ApplicationSpec appSpec;
    private final ApplicationRunner runner;

    AppRuntimeImpl(ApplicationSpec appSpec) {
      this.appSpec = appSpec;
      this.runner = ApplicationRunners.fromConfig(appSpec.getConfig());
    }

    @Override
    public void start() {
      this.runner.run(appSpec);
    }

    @Override
    public void stop() {
      this.runner.kill(appSpec);
    }

    @Override
    public ApplicationStatus status() {
      return this.runner.status(appSpec);
    }

    @Override
    public void waitForFinish() {
      this.runner.waitForFinish(appSpec, Duration.ofSeconds(0));
    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return this.runner.waitForFinish(appSpec, timeout);
    }

    @Override
    public void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
      this.runner.addMetricsReporters(metricsReporters);
    }
  }
}
