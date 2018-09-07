package org.apache.samza.context;

/**
 * An application should implement this to contain any runtime objects required to initialize the processing logic.
 * <p>
 * An instance of this will be created for each task.
 * <p>
 * If it is necessary to have a separate instance per task, then use {@link ApplicationDefinedContainerContext} instead.
 * An {@link ApplicationDefinedTaskContextFactory} needs to be implemented to build instances of this context.
 */
public interface ApplicationDefinedTaskContext {
  /**
   * Initialization which will run before processing begins.
   */
  void start();

  /**
   * Shutdown logic which will run after processing ends.
   */
  void stop();
}
