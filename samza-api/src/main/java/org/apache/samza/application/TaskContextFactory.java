package org.apache.samza.application;

import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;


/**
 * Created by yipan on 8/31/17.
 */
public interface TaskContextFactory {
  TaskContext getTaskContext(Config config, TaskContext taskContext);
}
