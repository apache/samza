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
package org.apache.samza.application.internal;

import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.ApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.runtime.ProcessorLifecycleListener;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.task.TaskContext;


/**
 * This is the base class that implements interface {@link ApplicationDescriptor}. This base class contains the common objects
 * that are used by both high-level and low-level API applications, such as {@link Config}, {@link ContextManager}, and
 * {@link ProcessorLifecycleListener}.
 */
public abstract class AppDescriptorImpl<T extends ApplicationBase, S extends ApplicationDescriptor<T>>
    implements ApplicationDescriptor<T> {

  final Config config;

  // Default to no-op functions in ContextManager
  // TODO: this should be replaced by shared context factory defined in SAMZA-1714
  ContextManager contextManager = new ContextManager() {
    @Override
    public void init(Config config, TaskContext context) {
    }

    @Override
    public void close() {
    }
  };

  // Default to no-op  ProcessorLifecycleListenerFactory
  ProcessorLifecycleListenerFactory listenerFactory = (pcontext, cfg) -> new ProcessorLifecycleListener() { };

  AppDescriptorImpl(Config config) {
    this.config = config;
  }

  static class AppConfig extends MapConfig {

    static final String APP_NAME = "app.name";
    static final String APP_ID = "app.id";

    static final String JOB_NAME = "job.name";
    static final String JOB_ID = "job.id";

    AppConfig(Config config) {
      super(config);
    }

    String getAppName() {
      return get(APP_NAME, get(JOB_NAME));
    }

    String getAppId() {
      return get(APP_ID, get(JOB_ID, "1"));
    }

    /**
     * Returns full application id
     *
     * @return full app id
     */
    String getGlobalAppId() {
      return String.format("app-%s-%s", getAppName(), getAppId());
    }

  }

  @Override
  public String getGlobalAppId() {
    return new AppConfig(config).getGlobalAppId();
  }

  @Override
  public Config getConfig() {
    return config;
  }

  @Override
  public S withContextManager(ContextManager contextManager) {
    this.contextManager = contextManager;
    return (S) this;
  }

  @Override
  public S withProcessorLifecycleListenerFactory(ProcessorLifecycleListenerFactory listenerFactory) {
    this.listenerFactory = listenerFactory;
    return (S) this;
  }

  /**
   * Get the user-implemented {@link ContextManager} object associated with this application
   *
   * @return the {@link ContextManager} object
   */
  public ContextManager getContextManager() {
    return contextManager;
  }

  /**
   * Get the user-implemented {@link ProcessorLifecycleListener} object associated with this application
   *
   * @return the {@link ProcessorLifecycleListener} object
   */
  public ProcessorLifecycleListenerFactory getProcessorLifecycleListenerFactory() {
    return listenerFactory;
  }

}