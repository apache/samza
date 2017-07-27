package org.apache.samza.application;

import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.ApplicationRunner;


/**
 * Created by yipan on 7/25/17.
 */
public class ApplicationBase {

  protected final ApplicationRunner runner;

  ApplicationBase(ApplicationRunner runner) {
    this.runner = runner;
  }

  /**
   * Deploy and run the Samza jobs to execute {@link StreamApplication}.
   * It is non-blocking so it doesn't wait for the application running.
   *
   */
  public void run() {
    this.runner.run(this);
  }

  /**
   * Kill the Samza jobs represented by {@link StreamApplication}
   * It is non-blocking so it doesn't wait for the application stopping.
   *
   */
  public void kill() {
    this.runner.kill(this);
  }

  /**
   * Get the collective status of the Samza jobs represented by {@link StreamApplication}.
   * Returns {@link ApplicationRunner} running if all jobs are running.
   *
   * @return the status of the application
   */
  public ApplicationStatus status() {
    return this.runner.status(this);
  }

  public void waitForFinish() {
    this.runner.waitForFinish();
  }

}
