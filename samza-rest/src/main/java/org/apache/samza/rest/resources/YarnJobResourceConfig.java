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
package org.apache.samza.rest.resources;

/**
 * Configurations for the YARN based {@link JobsResource} endpoint.
 */
public class YarnJobResourceConfig extends JobsResourceConfig {
  public YarnJobResourceConfig(JobsResourceConfig config) {
    super(config);
  }

  /**
   * Specifies the host and port of the YARN ResourceManager against against which the API
   * calls are made.
   */
  private static final String CONFIG_YARN_RESOURCE_MANAGER_API_ENDPOINT = "yarn.resourcemanager.api.endpoint";

  /**
   * Specifies the default host and port of the YARN ResourceManager assuming that samza-rest is running on the
   * Resource Manager host.
   */
  private static final String CONFIG_DEFAULT_YARN_RESOURCE_MANAGER_API_ENDPOINT = "localhost:8088";

  /**
   * @see YarnJobResourceConfig#CONFIG_YARN_RESOURCE_MANAGER_API_ENDPOINT
   * @return the ResourceManager host and port on which the REST API is exposed.
   */
  public String getYarnResourceManagerEndpoint() {
    String api = get(CONFIG_YARN_RESOURCE_MANAGER_API_ENDPOINT);
    if (api == null) {
      api = CONFIG_DEFAULT_YARN_RESOURCE_MANAGER_API_ENDPOINT;
    }
    return api;
  }
}
