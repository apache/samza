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
package org.apache.samza.rest.proxy.task;

import org.apache.samza.config.Config;


/**
 * Factory interface that will be used to create {@link TaskProxy}
 * instances.
 *
 * To use a custom {@link TaskProxy}, create an implementation of this interface
 * and instantiate the custom proxy in the getTaskProxy method. Set
 * the config {@link TaskResourceConfig#CONFIG_TASK_PROXY_FACTORY}
 * value to the appropriate factory implementation class.
 */
public interface TaskProxyFactory {

  /**
   *
   * @param config the {@link Config} to pass to the proxy.
   * @return the created proxy.
   */
  TaskProxy getTaskProxy(TaskResourceConfig config);
}
