package org.apache.samza.context;

/**
 * Container object for all context provided by the Samza framework to instantiate an application at runtime.
 */
public interface SamzaContext {
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
}
