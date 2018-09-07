package org.apache.samza.context;

public class SamzaContainerContextImpl implements SamzaContainerContext {
  private final JobContext jobContext;
  private final ContainerContext containerContext;

  public SamzaContainerContextImpl(JobContext jobContext, ContainerContext containerContext) {
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
}
