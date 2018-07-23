package org.apache.samza.application;

import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;


/**
 * Created by yipan on 7/19/18.
 */
public interface ApplicationSpec<T extends LifecycleAwareApplication> {
  /**
   * Get the global unique application ID in the runtime process
   * @return globally unique application ID
   */
  String getGlobalAppId();

  /**
   * Get the user defined {@link Config}
   * @return config object
   */
  Config getConfig();

  /**
   * Sets the {@link ContextManager} for this {@link StreamApplicationSpec}.
   * <p>
   * The provided {@link ContextManager} can be used to setup shared context between the operator functions
   * within a task instance
   *
   * @param contextManager the {@link ContextManager} to use for the {@link StreamApplicationSpec}
   * @return the {@link StreamApplicationSpec} with {@code contextManager} set as its {@link ContextManager}
   */
  ApplicationSpec<T> withContextManager(ContextManager contextManager);

}
