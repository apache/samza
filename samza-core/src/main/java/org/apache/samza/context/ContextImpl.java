package org.apache.samza.context;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;


public class ContextImpl implements Context {
  private final JobContext jobContext;
  private final ContainerContext containerContext;
  private final TaskContext taskContext;
  private final ApplicationDefinedContainerContext applicationDefinedContainerContext;
  private final ApplicationDefinedTaskContext applicationDefinedTaskContext;

  @VisibleForTesting
  ContextImpl(JobContext jobContext, ContainerContext containerContext, TaskContext taskContext,
      ApplicationDefinedContainerContext applicationDefinedContainerContext,
      ApplicationDefinedTaskContext applicationDefinedTaskContext) {
    this.jobContext = jobContext;
    this.containerContext = containerContext;
    this.taskContext = taskContext;
    this.applicationDefinedContainerContext = applicationDefinedContainerContext;
    this.applicationDefinedTaskContext = applicationDefinedTaskContext;
  }

  /**
   * This is built using a {@link ContextProvider}.
   */
  ContextImpl(SamzaContainerContext samzaContainerContext, TaskContext taskContext,
      ApplicationDefinedContainerContext applicationDefinedContainerContext,
      ApplicationDefinedTaskContext applicationDefinedTaskContext) {
    this(samzaContainerContext.getJobContext(), samzaContainerContext.getContainerContext(), taskContext,
        applicationDefinedContainerContext, applicationDefinedTaskContext);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContextImpl context = (ContextImpl) o;
    return Objects.equals(jobContext, context.jobContext) && Objects.equals(containerContext, context.containerContext)
        && Objects.equals(taskContext, context.taskContext) && Objects.equals(applicationDefinedContainerContext,
        context.applicationDefinedContainerContext) && Objects.equals(applicationDefinedTaskContext,
        context.applicationDefinedTaskContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobContext, containerContext, taskContext, applicationDefinedContainerContext,
        applicationDefinedTaskContext);
  }
}
