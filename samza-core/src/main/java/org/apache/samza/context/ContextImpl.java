package org.apache.samza.context;

public class ContextImpl implements Context {
  private final JobContext _jobContext;
  private final ContainerContext _containerContext;
  private final TaskContext _taskContext;
  private final ApplicationDefinedContainerContext _applicationDefinedContainerContext;
  private final ApplicationDefinedTaskContext _applicationDefinedTaskContext;

  public ContextImpl(JobContext jobContext, ContainerContext containerContext, TaskContext taskContext,
      ApplicationDefinedContainerContext applicationDefinedContainerContext,
      ApplicationDefinedTaskContext applicationDefinedTaskContext) {
    _jobContext = jobContext;
    _containerContext = containerContext;
    _taskContext = taskContext;
    _applicationDefinedContainerContext = applicationDefinedContainerContext;
    _applicationDefinedTaskContext = applicationDefinedTaskContext;
  }

  @Override
  public JobContext getJobContext() {
    return _jobContext;
  }

  @Override
  public ContainerContext getContainerContext() {
    return _containerContext;
  }

  @Override
  public TaskContext getTaskContext() {
    return _taskContext;
  }

  @Override
  public ApplicationDefinedContainerContext getApplicationDefinedContainerContext() {
    return _applicationDefinedContainerContext;
  }

  @Override
  public ApplicationDefinedTaskContext getApplicationDefinedTaskContext() {
    return _applicationDefinedTaskContext;
  }
}
