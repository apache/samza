package org.apache.samza.config;

public class JavaJobConfig extends MapConfig {
  private static final String JOB_NAME = "job.name"; // streaming.job_name
  private static final String JOB_ID = "job.id"; // streaming.job_id
  private static final String DEFAULT_JOB_ID = "1";

  public JavaJobConfig (Config config) {
    super(config);
  }

  public String getJobName() {
    if (!containsKey(JOB_NAME)) {
      throw new ConfigException("Missing " + JOB_NAME + " config!");
    }
    return get(JOB_NAME);
  }

  public String getJobName(String defaultValue) {
    return get(JOB_NAME, defaultValue);
  }

  public String getJobId() {
    return get(JOB_ID, DEFAULT_JOB_ID);
  }

}
