package org.apache.samza.runtime;

import java.time.Duration;
import java.util.Map;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;


/**
 * Created by yipan on 7/11/18.
 */
public interface ApplicationRuntime {
  /**
   * Start an application
   */
  void start();

  /**
   * Stop an application
   */
  void stop();

  /**
   * Get the {@link ApplicationStatus} of an application
   * @return the runtime status of the application
   */
  ApplicationStatus status();

  /**
   * Wait the application to complete.
   * This method will block until the application completes.
   */
  void waitForFinish();

  /**
   * Wait the application to complete with a {@code timeout}
   *
   * @param timeout the time to block to wait for the application to complete
   * @return true if the application completes within timeout; false otherwise
   */
  boolean waitForFinish(Duration timeout);

  /**
   * Method to add a set of customized {@link MetricsReporter}s in the application
   *
   * @param metricsReporters the map of customized {@link MetricsReporter}s objects to be used
   */
  void addMetricsReporters(Map<String, MetricsReporter> metricsReporters);

}
