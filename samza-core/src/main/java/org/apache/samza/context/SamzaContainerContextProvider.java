package org.apache.samza.context;

/**
 * Provides a way to build a {@link SamzaContainerContext} when given certain container context.
 */
public class SamzaContainerContextProvider {
  private final JobContext jobContext;

  public SamzaContainerContextProvider(JobContext jobContext) {
    this.jobContext = jobContext;
  }

  public SamzaContainerContext build(ContainerContext containerContext) {
    return new SamzaContainerContextImpl(this.jobContext, containerContext);
  }
}
