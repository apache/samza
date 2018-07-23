package org.apache.samza.application;

import java.util.List;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.task.TaskFactory;


/**
 * Created by yipan on 7/19/18.
 */
public interface TaskApplicationSpec extends ApplicationSpec<TaskApplication> {

  void setTaskFactory(TaskFactory factory);

  void addInputStreams(List<String> inputStreams);

  void addOutputStreams(List<String> outputStreams);

  void addTables(List<TableDescriptor> tables);

}
