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

package org.apache.samza.job;

import java.net.URL;
import java.util.Map;
import org.apache.samza.config.Config;


/**
 * CommandBuilders are used to customize the command necessary to launch a Samza
 * Job for a particular framework, such as YARN or the LocalJobRunner.
 */
public abstract class CommandBuilder {
  protected Config config;
  protected String containerId;
  protected URL url;
  protected String commandPath;

  /**
   * @param config The config object to use when constructing the command and environment.
   * @return self to support a builder style of use.
   */
  public CommandBuilder setConfig(Config config) {
    this.config = config;
    return this;
  }

  /**
   * @param url The URL location of the job coordinator's HTTP server, which serves all configuration, task/SSP assignments, etc.
   * @return self to support a builder style of use.
   */
  public CommandBuilder setUrl(URL url) {
    this.url = url;
    return this;
  }

  /**
   * @param containerId associated with a specific instantiation of a SamzaContainer.
   * @return self to support a builder style of use.
   */
  public CommandBuilder setId(String containerId) {
    this.containerId = containerId;
    return this;
  }

  /**
   * @param path Specify path to the command (in case it needs to be adjusted)
   * @return self
   */
  public CommandBuilder setCommandPath(String path) {
    this.commandPath = path;
    return this;
  }

  public abstract String buildCommand();

  public abstract Map<String, String> buildEnvironment();
}
