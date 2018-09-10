package org.apache.samza.context;

import java.util.Objects;


public class SamzaContainerContextImpl implements SamzaContainerContext {
  private final JobContext jobContext;
  private final ContainerContext containerContext;

  /**
   * This is built using a {@link SamzaContainerContextProvider}.
   */
  SamzaContainerContextImpl(JobContext jobContext, ContainerContext containerContext) {
    this.jobContext = jobContext;
    this.containerContext = containerContext;
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SamzaContainerContextImpl that = (SamzaContainerContextImpl) o;
    return Objects.equals(jobContext, that.jobContext) && Objects.equals(containerContext, that.containerContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobContext, containerContext);
  }
}
