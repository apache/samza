package org.apache.samza.application.internal;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.TaskApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.task.TaskFactory;


/**
 * Created by yipan on 7/10/18.
 */
public class TaskAppSpecImpl extends AppSpecImpl<TaskApplication, TaskApplicationSpec> implements TaskApplicationSpec {

  TaskFactory taskFactory;
  final List<String> inputStreams = new ArrayList<>();
  final List<String> outputStreams = new ArrayList<>();
  final List<TableDescriptor> tables = new ArrayList<>();

  public TaskAppSpecImpl(TaskApplication userApp, Config config) {
    super(config);
    userApp.describe(this);
  }

  @Override
  public void setTaskFactory(TaskFactory factory) {
    this.taskFactory = factory;
  }

  @Override
  public void addInputStreams(List<String> inputStreams) {
    this.inputStreams.addAll(inputStreams);
  }

  @Override
  public void addOutputStreams(List<String> outputStreams) {
    this.outputStreams.addAll(outputStreams);
  }

  @Override
  public void addTables(List<TableDescriptor> tables) {
    this.tables.addAll(tables);
  }

  public TaskFactory getTaskFactory() {
    return taskFactory;
  }
}
