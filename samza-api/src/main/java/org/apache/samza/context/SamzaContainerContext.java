package org.apache.samza.context;

/**
 * Container object for job and container context provided by the Samza framework to instantiate an application at
 * runtime.
 */
public interface SamzaContainerContext {
  /**
   * @return framework-provided context for the overall job that is being run
   */
  JobContext getJobContext();

  /**
   * @return framework-provided context for the container that this is in
   */
  ContainerContext getContainerContext();
}
