package org.apache.samza.application.internal;

import org.apache.samza.task.TaskFactory;


/**
 * Created by yipan on 7/10/18.
 */
public class TaskApplicationRuntime {
  private final TaskApplicationSpec appSpec;

  public TaskApplicationRuntime(TaskApplicationSpec app) {
    this.appSpec = app;
  }

  public TaskFactory getTaskFactory() {
    return (TaskFactory) this.appSpec.taskFactory;
  }

}
