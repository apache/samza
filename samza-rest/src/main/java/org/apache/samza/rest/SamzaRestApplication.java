/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.rest;

import java.util.Collection;
import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.rest.resources.DefaultResourceFactory;
import org.apache.samza.rest.resources.ResourceFactory;
import org.apache.samza.util.ClassLoaderHelper;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Samza REST implementation of the JAX-RS {@link javax.ws.rs.core.Application} model.
 */
public class SamzaRestApplication extends ResourceConfig {

  private static final Logger log = LoggerFactory.getLogger(SamzaRestApplication.class);

  public SamzaRestApplication(SamzaRestConfig config) {
    register(JacksonJsonProvider.class);
    registerConfiguredResources(config);
  }

  /**
   * Registers resources specified in the config. If there are no factories
   * or resources specified in the config, it uses the
   * {@link org.apache.samza.rest.resources.DefaultResourceFactory}
   *
   * @param config  the config to pass to the factories.
   */
  private void registerConfiguredResources(SamzaRestConfig config) {
    try {
      // Use default if there were no configured resources or factories
      if (config.getResourceFactoryClassNames().isEmpty() && config.getResourceClassNames().isEmpty()) {
        log.info("No resource factories or classes configured. Using DefaultResourceFactory.");
        registerInstances(new DefaultResourceFactory().getResourceInstances(config).toArray());
        return;
      }

      for (String factoryClassName : config.getResourceFactoryClassNames()) {
        log.info("Invoking factory {}", factoryClassName);
        registerInstances(instantiateFactoryResources(factoryClassName, config).toArray());
      }

      for (String resourceClassName : config.getResourceClassNames()) {
        log.info("Using resource class {}", resourceClassName);
        register(Class.forName(resourceClassName));
      }
    } catch (Throwable t) {
      throw new SamzaException(t);
    }
  }

  /**
   * Passes the specified config to the specified factory to instantiate its resources.
   *
   * @param factoryClassName  the name of a class that implements {@link ResourceFactory}
   * @param config            the config to pass to the factory
   * @return                  a collection of resources returned by the factory.
   * @throws InstantiationException
   */
  private Collection<? extends Object> instantiateFactoryResources(String factoryClassName, Config config)
      throws InstantiationException {
    try {
      ResourceFactory factory = ClassLoaderHelper.<ResourceFactory>fromClassName(factoryClassName);
      return factory.getResourceInstances(config);
    } catch (Exception e) {
      throw (InstantiationException)
          new InstantiationException("Unable to instantiate " + factoryClassName).initCause(e);
    }
  }
}
