package org.apache.samza.runtime;

import java.time.Duration;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.internal.StreamApplicationBuilder;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.internal.TaskApplicationBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.apache.samza.runtime.internal.ApplicationRunners;
import org.apache.samza.runtime.internal.ApplicationSpec;


/**
 * Created by yipan on 7/11/18.
 */
public class ApplicationRuntimes {
  private ApplicationRuntimes() {

  }

  public static final ApplicationRuntime createStreamApp(StreamApplication userApp, Config config) {
    return new RuntimeAppImpl(new StreamApplicationBuilder(userApp, config));
  }

  public static final ApplicationRuntime createTaskApp(TaskApplication userApp, Config config) {
    return new RuntimeAppImpl(new TaskApplicationBuilder(userApp, config));
  }


  private static class RuntimeAppImpl implements ApplicationRuntime {
    private final ApplicationSpec appSpec;
    private final ApplicationRunner runner;

    RuntimeAppImpl(ApplicationSpec appSpec) {
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
