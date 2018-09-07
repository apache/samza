package org.apache.samza.context;

import java.io.Serializable;


/**
 * An application should implement this if it has a {@link ApplicationDefinedContainerContext} that is needed for
 * initialization. This will be used to create instance(s) of that {@link ApplicationDefinedContainerContext}.
 * <p>
 * This is {@link Serializable} because it is specified in {@link org.apache.samza.application.ApplicationDescriptor}.
 * @param <T> concrete type of {@link ApplicationDefinedContainerContext} returned by this factory
 */
public interface ApplicationDefinedContainerContextFactory<T extends ApplicationDefinedContainerContext>
    extends Serializable {
  /**
   * Create an instance of the application-defined {@link ApplicationDefinedContainerContext}.
   * @param samzaContext context that can be used to build the {@link ApplicationDefinedContainerContext}
   * @return new instance of the application-defined {@link ApplicationDefinedContainerContext}
   */
  T create(SamzaContext samzaContext);
}
