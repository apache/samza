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
package org.apache.samza.rest.proxy.job;

import org.apache.samza.rest.resources.JobsResourceConfig;


/**
 * Simple factory interface to produce instances of {@link JobProxy},
 * depending on the implementation.
 *
 * To use a custom {@link JobProxy}, create an implementation of that interface, an implementation
 * of this interface which instantiates the custom proxy and finally reference the custom factory
 * in the config {@link JobsResourceConfig#CONFIG_JOB_PROXY_FACTORY}.
 */
public interface JobProxyFactory {

  /**
   * Creates a new {@link JobProxy} and initializes it with the specified config.
   *
   * @param config  the {@link org.apache.samza.rest.SamzaRestConfig} to pass to the proxy.
   * @return        the created proxy.
   */
  JobProxy getJobProxy(JobsResourceConfig config);
}
