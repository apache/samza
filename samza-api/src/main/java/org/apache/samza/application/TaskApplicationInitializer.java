package org.apache.samza.application;

import java.util.List;
import org.apache.samza.task.TaskFactory;


/**
 * Created by yipan on 7/18/18.
 */
public interface TaskApplicationInitializer extends ApplicationInitializer<TaskApplication> {
  void setTaskFactory(TaskFactory factory);

  void addInputStreams(List<String> inputStreams);

  void addOutputStreams(List<String> outputStreams);

  void addTables(List<String> tables);

}
