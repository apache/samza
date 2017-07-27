package org.apache.samza.application;

import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.task.AsyncStreamTaskFactory;


/**
 * Created by yipan on 7/24/17.
 */
public class AsyncStreamTaskApplication extends ApplicationBase {
  private final AsyncStreamTaskFactory taskFactory;

  private AsyncStreamTaskApplication(AsyncStreamTaskFactory taskFactory, ApplicationRunner runner) {
    super(runner);
    this.taskFactory = taskFactory;
  }

  public static AsyncStreamTaskApplication create(Config config, AsyncStreamTaskFactory taskFactory) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    return new AsyncStreamTaskApplication(taskFactory, runner);
  }

  public AsyncStreamTaskApplication addInputs(List<StreamDescriptor.Input> inputs) {
    return this;
  }

  public AsyncStreamTaskApplication addOutputs(List<StreamDescriptor.Output> outputs) {
    return this;
  }

}
