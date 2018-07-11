package org.apache.samza.application.internal;

import org.apache.samza.config.Config;
import org.apache.samza.task.TaskFactory;


/**
 * Created by yipan on 7/10/18.
 */
public class TaskApplicationSpec extends ApplicationSpec {

  final TaskFactory taskFactory;

  public TaskApplicationSpec(TaskFactory taskFactory, Config config) {
    super(config);
    this.taskFactory = taskFactory;
  }

  public TaskFactory getTaskFactory() {
    return this.taskFactory;
  }
}
