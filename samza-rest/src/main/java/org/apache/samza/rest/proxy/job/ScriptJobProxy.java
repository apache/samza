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

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import org.apache.samza.rest.proxy.installation.InstallationFinder;
import org.apache.samza.rest.proxy.installation.InstallationRecord;
import org.apache.samza.rest.resources.JobsResourceConfig;
import org.apache.samza.rest.script.ScriptPathProvider;
import org.apache.samza.rest.script.ScriptRunner;

/**
 * Extends {@link AbstractJobProxy} with some script support functionality.
 */
public abstract class ScriptJobProxy extends AbstractJobProxy implements ScriptPathProvider {

  protected final ScriptRunner scriptRunner = new ScriptRunner();

  /**
   * Required constructor.
   *
   * @param config  the config which specifies the path to the Samza framework installation.
   */
  public ScriptJobProxy(JobsResourceConfig config) {
    super(config);
  }

  /**
   * Constructs the path to the specified script within the job installation.
   *
   * @param jobInstance             the instance of the job.
   * @param scriptName              the name of the script.
   * @return                        the full path to the script.
   * @throws FileNotFoundException  if the job installation path doesn't exist.
   */
  @Override
  public String getScriptPath(JobInstance jobInstance, String scriptName)
      throws FileNotFoundException {
    String scriptPath;
    InstallationRecord jobInstallation = getInstallationFinder().getAllInstalledJobs().get(jobInstance);
    scriptPath = Paths.get(jobInstallation.getScriptFilePath(), scriptName).toString();

    File scriptFile = new File(scriptPath);
    if (!scriptFile.exists()) {
      throw new FileNotFoundException("Script does not exist: " + scriptPath);
    }
    return scriptPath;
  }

  /**
   * @return the {@link InstallationFinder} which will be used to find jobs installed on this machine.
   */
  protected abstract InstallationFinder getInstallationFinder();
}
