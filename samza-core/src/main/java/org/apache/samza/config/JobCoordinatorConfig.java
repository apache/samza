package org.apache.samza.config;

import com.google.common.base.Strings;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.util.Util;

public class JobCoordinatorConfig extends MapConfig {
  // TODO: Change this to job-coordinator.factory
  private static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";

  public JobCoordinatorConfig (Config config) {
    super(config);
  }

  public String getJobCoordinatorFactoryClassName() {
    String jobCoordinatorFactoryClassName = get(JOB_COORDINATOR_FACTORY);
    if (Strings.isNullOrEmpty(jobCoordinatorFactoryClassName)) {
      throw new ConfigException(
          String.format("Missing config - %s. Cannot start StreamProcessor!", JOB_COORDINATOR_FACTORY));
    }

    return jobCoordinatorFactoryClassName;
  }
}
