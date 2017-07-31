package org.apache.samza.application;

import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;


/**
 * Created by yipan on 7/28/17.
 */
public class AsyncStreamTaskApplicationInternal {

  private final AsyncStreamTaskApplication app;

  public AsyncStreamTaskApplicationInternal(AsyncStreamTaskApplication app) {
    this.app = app;
  }

  public AsyncStreamTaskFactory getTaskFactory() {
    return this.app.taskFactory;
  }

}
