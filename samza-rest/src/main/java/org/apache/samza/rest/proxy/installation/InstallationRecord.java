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
package org.apache.samza.rest.proxy.installation;

import org.apache.samza.rest.proxy.job.JobInstance;


/**
 * Represents an installation of one Samza job instance on a file system.
 *
 * This class does not hard code any knowledge about the structure of a Samza job installation. Rather, it
 * just points to the relevant paths within the installation. The structure is resolved by an implementation
 * of the {@link InstallationFinder} interface.
 */
public class InstallationRecord extends JobInstance {

  private final String rootPath;
  private final String configFilePath;
  private final String binPath;

  public InstallationRecord(String jobName, String jobId, String rootPath, String configFilePath, String binPath) {
    super(jobName, jobId);
    this.rootPath = rootPath;
    this.configFilePath = configFilePath;
    this.binPath = binPath;
  }

  /**
   * @return  the path of the config file for the job.
   */
  public String getConfigFilePath() {
    return configFilePath;
  }

  /**
   * @return  the path of the directory containing the scripts.
   */
  public String getScriptFilePath() {
    return binPath;
  }

  /**
   * @return  the root path of the installed Samza job on the file system. This path may be in common with
   *          other job instances if, for example, there are multiple configs defining separate instances.
   */
  public String getRootPath() {
    return rootPath;
  }

  @Override
  public String toString() {
    return String.format("Job %s installed at %s", super.toString(), getRootPath());
  }
}
