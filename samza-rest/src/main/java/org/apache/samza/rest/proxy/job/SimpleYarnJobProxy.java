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

import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.model.yarn.YarnApplicationInfo;
import org.apache.samza.rest.proxy.installation.InstallationFinder;
import org.apache.samza.rest.proxy.installation.InstallationRecord;
import org.apache.samza.rest.proxy.installation.SimpleInstallationFinder;
import org.apache.samza.rest.resources.JobsResourceConfig;
import org.apache.samza.util.ClassLoaderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the {@link ScriptJobProxy} with methods specific to simple Samza deployments.
 */
public class SimpleYarnJobProxy extends ScriptJobProxy {
  private static final Logger log = LoggerFactory.getLogger(SimpleYarnJobProxy.class);

  private static final String START_SCRIPT_NAME = "run-job.sh";
  private static final String STOP_SCRIPT_NAME = "kill-yarn-job-by-name.sh";

  private static final String CONFIG_FACTORY_PARAM = "--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory";
  private static final String CONFIG_PATH_PARAM_FORMAT = "--config-path=file://%s";

  private final JobStatusProvider statusProvider;

  private final InstallationFinder installFinder;

  public SimpleYarnJobProxy(JobsResourceConfig config) throws Exception {
    super(config);
    this.installFinder = new SimpleInstallationFinder(config.getInstallationsPath(),
                                                      ClassLoaderHelper.fromClassName(config.getJobConfigFactory()));
    this.statusProvider = new YarnRestJobStatusProvider(config);
  }

  @Override
  public void start(JobInstance jobInstance)
      throws Exception {
    JobStatus currentStatus = getJobSamzaStatus(jobInstance);
    if (currentStatus.hasBeenStarted()) {
      log.info("Job {} will not be started because it is currently {}.", jobInstance, currentStatus.toString());
      return;
    }

    String scriptPath = getScriptPath(jobInstance, START_SCRIPT_NAME);
    int resultCode = scriptRunner.runScript(scriptPath, CONFIG_FACTORY_PARAM,
        generateConfigPathParameter(jobInstance));
    if (resultCode != 0) {
      throw new SamzaException("Failed to start job. Result code: " + resultCode);
    }
  }

  @Override
  public void stop(JobInstance jobInstance)
      throws Exception {
    JobStatus currentStatus = getJobSamzaStatus(jobInstance);
    if (!currentStatus.hasBeenStarted()) {
      log.info("Job {} will not be stopped because it is currently {}.", jobInstance, currentStatus.toString());
      return;
    }

    String scriptPath = getScriptPath(jobInstance, STOP_SCRIPT_NAME);
    int resultCode = scriptRunner.runScript(scriptPath, YarnApplicationInfo.getQualifiedJobName(jobInstance));
    if (resultCode != 0) {
      throw new SamzaException("Failed to stop job. Result code: " + resultCode);
    }
  }

  /**
   * Generates the command line argument which specifies the path to the config file for the job.
   *
   * @param jobInstance the instance of the job.
   * @return            the --config-path command line argument.
   */
  private String generateConfigPathParameter(JobInstance jobInstance) {
    InstallationRecord record = installFinder.getAllInstalledJobs().get(jobInstance);
    return String.format(CONFIG_PATH_PARAM_FORMAT, record.getConfigFilePath());
  }

  /**
   * @return the {@link JobStatusProvider} to use for retrieving job status.
   */
  public JobStatusProvider getJobStatusProvider() {
    return statusProvider;
  }

  @Override
  protected Set<JobInstance> getAllJobInstances() {
    return installFinder.getAllInstalledJobs().keySet();
  }

  @Override
  protected InstallationFinder getInstallationFinder() {
    return installFinder;
  }
}
