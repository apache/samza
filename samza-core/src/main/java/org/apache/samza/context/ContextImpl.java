package org.apache.samza.context;

public class ContextImpl implements Context {
  private final JobContext jobContext;
  private final ContainerContext containerContext;
  private final TaskContext taskContext;
  private final ApplicationDefinedContainerContext applicationDefinedContainerContext;
  private final ApplicationDefinedTaskContext applicationDefinedTaskContext;

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
    this.jobContext = jobContext;
    this.containerContext = containerContext;
    this.taskContext = taskContext;
    this.applicationDefinedContainerContext = applicationDefinedContainerContext;
    this.applicationDefinedTaskContext = applicationDefinedTaskContext;
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

  @Override
  public <T extends ApplicationDefinedContainerContext> T getApplicationDefinedContainerContext(Class<T> clazz) {
    if (this.applicationDefinedContainerContext == null) {
      throw new IllegalStateException("No application-defined container context exists");
    }
    return clazz.cast(this.applicationDefinedContainerContext);
  }

  @Override
  public <T extends ApplicationDefinedTaskContext> T getApplicationDefinedTaskContext(Class<T> clazz) {
    if (this.applicationDefinedTaskContext == null) {
      throw new IllegalStateException("No application-defined container context exists");
    }
    return clazz.cast(this.applicationDefinedTaskContext);
  }
}
