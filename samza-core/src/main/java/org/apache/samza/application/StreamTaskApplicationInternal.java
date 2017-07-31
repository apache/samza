package org.apache.samza.application;

import org.apache.samza.config.JobConfig;
import org.apache.samza.task.StreamTaskFactory;


/**
 * Created by yipan on 7/28/17.
 */
public class StreamTaskApplicationInternal {
  private final StreamTaskApplication app;

  public StreamTaskApplicationInternal(StreamTaskApplication app) {
    this.app = app;
  }

  public StreamTaskFactory getTaskFactory() {
    return this.app.taskFactory;
  }

}
