package org.apache.samza.context;

/**
 * Container object for all context provided to instantiate an application at runtime.
 */
public interface Context {
  /**
   * @return framework-provided context for the overall job that is being run
   */
  JobContext getJobContext();

  /**
   * @return framework-provided context for the container that this is in
   */
  ContainerContext getContainerContext();

  /**
   * @return framework-provided context for the task that that this is in
   */
  TaskContext getTaskContext();

  /**
   * Returns the application-defined container context object specified by the
   * {@link ApplicationDefinedContainerContextFactory}. This is shared across all tasks in the container, but not across
   * containers.
   * @param clazz concrete type of the {@link ApplicationDefinedContainerContext} to return
   * @return application-defined container context
   * @throws IllegalStateException if no factory was provided
   */
  <T extends ApplicationDefinedContainerContext> T getApplicationDefinedContainerContext(T clazz);

  /**
   * Returns the application-defined task context object specified by the {@link ApplicationDefinedTaskContextFactory}.
   * Each task will have a separate instance of this.
   * @param clazz concrete type of the {@link ApplicationDefinedTaskContext} to return
   * @return application-defined task context
   * @throws IllegalStateException if no factory was provided
   */
  <T extends ApplicationDefinedTaskContext> T getApplicationDefinedTaskContext(T clazz);
}
