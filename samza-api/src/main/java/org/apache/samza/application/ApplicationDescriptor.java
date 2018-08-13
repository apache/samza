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
package org.apache.samza.application;

import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;


/**
 * The base interface class to describe a user application in Samza. Sub-classes {@link StreamAppDescriptor}
 * and {@link TaskAppDescriptor} are specific interfaces for applications written in high-level DAG and low-level task APIs,
 * respectively.
 */
public interface ApplicationDescriptor<T extends ApplicationBase> {
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
   * Sets the {@link ContextManager} for this application.
   * <p>
   * The provided {@link ContextManager} can be used to setup shared context between the operator functions
   * within a task instance
   *
   * @param contextManager the {@link ContextManager} to use for the application
   * @return the {@link ApplicationDescriptor} with {@code contextManager} set as its {@link ContextManager}
   */
  ApplicationDescriptor<T> withContextManager(ContextManager contextManager);

  /**
   * Sets the {@link ProcessorLifecycleListenerFactory} for this application.
   *
   * @param listenerFactory the user implemented {@link ProcessorLifecycleListenerFactory} that creates lifecycle aware
   *                        methods to be invoked before and after the start/stop of the StreamProcessor(s) in the application
   * @return the {@link ApplicationDescriptor} with {@code listenerFactory} set as its {@link ProcessorLifecycleListenerFactory}
   */
  ApplicationDescriptor<T> withProcessorLifecycleListenerFactory(ProcessorLifecycleListenerFactory listenerFactory);

}
