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


/**
 * The base interface class to create the specification of a user application in Samza. Sub-classes {@link StreamApplicationSpec}
 * and {@link TaskApplicationSpec} are specific interfaces for applications written in high-level DAG and low-level task APIs,
 * respectively.
 */
public interface ApplicationSpec<T extends ApplicationBase> {
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
   * Sets the {@link ContextManager} for this {@link ApplicationSpec}.
   * <p>
   * The provided {@link ContextManager} can be used to setup shared context between the operator functions
   * within a task instance
   *
   * @param contextManager the {@link ContextManager} to use for the {@link StreamApplicationSpec}
   * @return the {@link ApplicationSpec} with {@code contextManager} set as its {@link ContextManager}
   */
  ApplicationSpec<T> withContextManager(ContextManager contextManager);

  /**
   * Sets the {@link ProcessorLifecycleListener} for this {@link ApplicationSpec}.
   *
   * @param listener the user implemented {@link ProcessorLifecycleListener} with lifecycle aware methods to be invoked
   *                 before and after the start/stop of the processing logic defined in this {@link ApplicationSpec}
   * @return the {@link ApplicationSpec} with {@code listener} set as its {@link ProcessorLifecycleListener}
   */
  ApplicationSpec<T> withProcessorLifecycleListener(ProcessorLifecycleListener listener);

}
