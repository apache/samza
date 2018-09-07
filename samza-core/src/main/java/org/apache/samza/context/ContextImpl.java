package org.apache.samza.context;

public class ContextImpl implements Context {
  private final JobContext _jobContext;
  private final ContainerContext _containerContext;
  private final TaskContext _taskContext;
  private final ApplicationDefinedContainerContext _applicationDefinedContainerContext;
  private final ApplicationDefinedTaskContext _applicationDefinedTaskContext;

  /**
   * @param jobContext non-null job context
   * @param containerContext non-null framework container context
   * @param taskContext non-null framework task context
   * @param applicationDefinedContainerContext nullable application-defined container context
   * @param applicationDefinedTaskContext nullable application-defined task context
   */
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
  public <T extends ApplicationDefinedContainerContext> T getApplicationDefinedContainerContext(Class<T> clazz) {
    if (_applicationDefinedContainerContext == null) {
      throw new IllegalStateException("No application-defined container context exists");
    }
    return clazz.cast(_applicationDefinedContainerContext);
  }

  @Override
  public <T extends ApplicationDefinedTaskContext> T getApplicationDefinedTaskContext(Class<T> clazz) {
    if (_applicationDefinedTaskContext == null) {
      throw new IllegalStateException("No application-defined container context exists");
    }
    return clazz.cast(_applicationDefinedTaskContext);
  }
}
