package org.apache.samza.runtime;

import java.time.Duration;
import java.util.Map;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;


/**
 * Created by yipan on 7/11/18.
 */
public interface ApplicationRuntime {
  void start();
  void stop();
  ApplicationStatus status();
  void waitForFinish();
  boolean waitForFinish(Duration timeout);

  /**
   * Method to add a set of customized {@link MetricsReporter}s in the application
   *
   * @param metricsReporters the map of customized {@link MetricsReporter}s objects to be used
   */
  void addMetricsReporters(Map<String, MetricsReporter> metricsReporters);

}
