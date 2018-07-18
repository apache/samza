package org.apache.samza.application.internal;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.TaskApplicationInitializer;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.internal.TaskApplicationSpec;
import org.apache.samza.task.TaskFactory;


/**
 * Created by yipan on 7/10/18.
 */
public class TaskApplicationBuilder extends ApplicationBuilder<TaskApplication> implements TaskApplicationInitializer, TaskApplicationSpec {

  TaskFactory taskFactory;
  final List<String> inputStreams = new ArrayList<>();
  final List<String> outputStreams = new ArrayList<>();
  final List<String> tables = new ArrayList<>();

  public TaskApplicationBuilder(TaskApplication userApp, Config config) {
    super(userApp, config);
    userApp.init(this, config);
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
  public void addTables(List<String> tables) {
    this.tables.addAll(tables);
  }

  @Override
  public TaskFactory getTaskFactory() {
    return this.taskFactory;
  }

  @Override
  public List<String> getInputStreams() {
    return this.inputStreams;
  }

  @Override
  public List<String> getOutputStreams() {
    return this.outputStreams;
  }

  @Override
  public List<String> getTables() {
    return this.tables;
  }
}
