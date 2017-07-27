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
public class StreamTaskApplication extends ApplicationBase {

  private final StreamTaskFactory taskFactory;

  private StreamTaskApplication(StreamTaskFactory taskFactory, ApplicationRunner runner) {
    super(runner);
    this.taskFactory = taskFactory;
  }

  public static StreamTaskApplication create(Config config, StreamTaskFactory taskFactory) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    return new StreamTaskApplication(taskFactory, runner);
  }

  public StreamTaskApplication addInputs(List<StreamDescriptor.Input> inputs) {
    return this;
  }

  public StreamTaskApplication addOutputs(List<StreamDescriptor.Output> outputs) {
    return this;
  }

}
