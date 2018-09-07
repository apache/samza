package org.apache.samza.context;

/**
 * An application should implement this to contain any runtime objects required to initialize the processing logic.
 * <p>
 * A single instance of this will be created in each container, so it will be shared between all of the tasks on a
 * single container.
 * <p>
 * If it is necessary to have a separate instance per task, then use {@link ApplicationDefinedTaskContext} instead.
 * An {@link ApplicationDefinedContainerContextFactory} needs to be implemented to build instances of this context.
 */
public interface ApplicationDefinedContainerContext {
  /**
   * Initialization which will run before processing begins.
   */
  void start();

  /**
   * Shutdown logic which will run after processing ends.
   */
  void stop();
}
