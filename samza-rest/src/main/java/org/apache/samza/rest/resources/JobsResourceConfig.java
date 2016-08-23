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

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.rest.proxy.installation.InstallationRecord;
import org.apache.samza.rest.proxy.job.JobProxy;
import org.apache.samza.rest.proxy.job.JobProxyFactory;


/**
 * Configurations for the {@link JobsResource} endpoint.
 */
public class JobsResourceConfig extends MapConfig {
  /**
   * Specifies the canonical name of the {@link JobProxyFactory} class to produce
   * {@link JobProxy} instances.
   *
   * To use your own proxy, implement the factory and specify the class for this config.
   */
  public static final String CONFIG_JOB_PROXY_FACTORY = "job.proxy.factory.class";

  /**
   * The path where all the Samza jobs are installed (unzipped). Each subdirectory of this path
   * is expected to be a Samza job installation and corresponds to one {@link InstallationRecord}.
   */
  public static final String CONFIG_JOB_INSTALLATIONS_PATH = "job.installations.path";

  /**
   * Specifies the canonical name of the {@link org.apache.samza.config.ConfigFactory} to read the job configs.
   */
  public static final String CONFIG_JOB_CONFIG_FACTORY = "job.config.factory.class";

  public JobsResourceConfig(Config config) {
    super(config);
  }

  /**
   * @see JobsResourceConfig#CONFIG_JOB_CONFIG_FACTORY
   * @return the canonical name of the {@link JobProxyFactory} class to produce {@link JobProxy} instances.
   */
  public String getJobProxyFactory() {
    return get(CONFIG_JOB_PROXY_FACTORY);
  }

  /**
   * @see JobsResourceConfig#CONFIG_JOB_INSTALLATIONS_PATH
   * @return the path where all the Samza jobs are installed (unzipped).
   */
  public String getInstallationsPath() {
    return sanitizePath(get(CONFIG_JOB_INSTALLATIONS_PATH));
  }

  /**
   * Ensures a usable file path when the user specifies a tilde for the home path.
   *
   * @param rawPath the original path.
   * @return        the updated path with the tilde resolved to home.
   */
  private static String sanitizePath(String rawPath) {
    if (rawPath == null) {
      return null;
    }
    return rawPath.replaceFirst("^~", System.getProperty("user.home"));
  }

}
