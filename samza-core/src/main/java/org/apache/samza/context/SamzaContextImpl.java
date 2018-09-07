package org.apache.samza.context;

public class SamzaContextImpl implements SamzaContext {
  private final JobContext jobContext;
  private final ContainerContext containerContext;
  private final TaskContext taskContext;

  public SamzaContextImpl(JobContext jobContext, ContainerContext containerContext, TaskContext taskContext) {
    this.jobContext = jobContext;
    this.containerContext = containerContext;
    this.taskContext = taskContext;
  }

  @Override
  public JobContext getJobContext() {
    return this.jobContext;
  }

  @Override
  public ContainerContext getContainerContext() {
    return this.containerContext;
  }

  @Override
  public TaskContext getTaskContext() {
    return this.taskContext;
  }
}
