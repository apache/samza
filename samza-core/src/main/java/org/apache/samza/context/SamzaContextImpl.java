package org.apache.samza.context;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;


public class SamzaContextImpl implements SamzaContext {
  private final JobContext jobContext;
  private final ContainerContext containerContext;
  private final TaskContext taskContext;

  @VisibleForTesting
  SamzaContextImpl(JobContext jobContext, ContainerContext containerContext, TaskContext taskContext) {
    this.jobContext = jobContext;
    this.containerContext = containerContext;
    this.taskContext = taskContext;
  }

  /**
   * This is built using a {@link SamzaContextProvider}.
   */
  SamzaContextImpl(SamzaContainerContext samzaContainerContext, TaskContext taskContext) {
    this(samzaContainerContext.getJobContext(), samzaContainerContext.getContainerContext(), taskContext);
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SamzaContextImpl that = (SamzaContextImpl) o;
    return Objects.equals(jobContext, that.jobContext)
        && Objects.equals(containerContext, that.containerContext)
        && Objects.equals(taskContext, that.taskContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobContext, containerContext, taskContext);
  }
}
