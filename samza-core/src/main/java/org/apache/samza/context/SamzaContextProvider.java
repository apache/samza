package org.apache.samza.context;

/**
 * Provides a way to build a {@link SamzaContext} when given certain task context objects.
 */
public class SamzaContextProvider {
  private final SamzaContainerContext samzaContainerContext;

  public SamzaContextProvider(SamzaContainerContext samzaContainerContext) {
    this.samzaContainerContext = samzaContainerContext;
  }

  public SamzaContext build(TaskContext taskContext) {
    return new SamzaContextImpl(this.samzaContainerContext, taskContext);
  }
}
