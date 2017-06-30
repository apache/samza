package org.apache.samza.application;

import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;

import java.util.List;

/**
 * Created by yipan on 6/21/17.
 */
public class StreamTaskApplication {

  private final StreamTaskFactory taskFactory;
  private final ApplicationRunner runner;

  private StreamTaskApplication(StreamTaskFactory taskFactory, ApplicationRunner runner) {
    this.taskFactory = taskFactory;
    this.runner = runner;
  }

  public static StreamTaskApplication create(Config config, StreamTaskFactory taskFactory) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    return new StreamTaskApplication(taskFactory, runner);
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

  public StreamTaskApplication addInputs(List<StreamDescriptor> inputs) {
    return this;
  }

  public StreamTaskApplication addOutputs(List<StreamDescriptor> outputs) {
    return this;
  }

}
