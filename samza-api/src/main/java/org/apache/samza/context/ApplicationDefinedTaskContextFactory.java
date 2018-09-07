package org.apache.samza.context;

import java.io.Serializable;


/**
 * An application should implement this if it has a {@link ApplicationDefinedTaskContext} that is needed for
 * initialization. This will be used to create instance(s) of that {@link ApplicationDefinedTaskContext}.
 * <p>
 * This is {@link Serializable} because it is specified in {@link org.apache.samza.application.ApplicationDescriptor}.
 * @param <T> concrete type of {@link ApplicationDefinedTaskContext} returned by this factory
 */
public interface ApplicationDefinedTaskContextFactory<T extends ApplicationDefinedTaskContext> extends Serializable {
  /**
   * Create an instance of the application-defined {@link ApplicationDefinedTaskContext}.
   * @param samzaContext context that can be used to build the {@link ApplicationDefinedTaskContext}
   * @return new instance of the application-defined {@link ApplicationDefinedContainerContext}
   */
  T create(SamzaContext samzaContext);
}
