package org.apache.samza.context;

/**
 * Provides a way to build a {@link Context} when given certain task context objects.
 */
public class ContextProvider {
  private final SamzaContainerContext samzaContainerContext;
  private final ApplicationDefinedContainerContext applicationDefinedContainerContext;

  /**
   * @param samzaContainerContext non-null {@link SamzaContainerContext}
   * @param applicationDefinedContainerContext nullable application-defined container context
   */
  public ContextProvider(SamzaContainerContext samzaContainerContext,
      ApplicationDefinedContainerContext applicationDefinedContainerContext) {
    this.samzaContainerContext = samzaContainerContext;
    this.applicationDefinedContainerContext = applicationDefinedContainerContext;
  }

  /**
   * @param taskContext non-null framework task context
   * @param applicationDefinedTaskContext nullable application-defined task context
   */
  public Context build(TaskContext taskContext, ApplicationDefinedTaskContext applicationDefinedTaskContext) {
    return new ContextImpl(this.samzaContainerContext, taskContext, this.applicationDefinedContainerContext,
        applicationDefinedTaskContext);
  }
}
