package org.apache.samza.runtime.internal;

import java.util.List;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.task.TaskFactory;


/**
 * Created by yipan on 7/17/18.
 */
public interface TaskApplicationSpec extends ApplicationSpec<TaskApplication> {
  TaskFactory getTaskFactory();

  List<String> getInputStreams();

  List<String> getOutputStreams();

  List<String> getTables();

}
