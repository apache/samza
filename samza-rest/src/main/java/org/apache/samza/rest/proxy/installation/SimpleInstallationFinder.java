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

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigFactory;
import org.apache.samza.config.JobConfig;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple default implementation of {@link InstallationFinder}.
 *
 * Assumes that one or more Samza jobs are contained in each sub directory of the provided installationsPath.
 * Each sub directory is also expected to contian a bin directory and a config directory containing one or
 * more job config files.
 */
public class SimpleInstallationFinder implements InstallationFinder {
  private static final Logger log = LoggerFactory.getLogger(SimpleInstallationFinder.class);

  protected static final String BIN_SUBPATH = "bin";
  protected static final String CFG_SUBPATH = "config";

  protected final String installationsPath;
  protected final ConfigFactory jobConfigFactory;

  /**
   * Required constructor.
   *
   * @param installationsPath the root path where all Samza jobs are installed.
   * @param jobConfigFactory  the {@link ConfigFactory} to use to read the job configs.
   */
  public SimpleInstallationFinder(String installationsPath, ConfigFactory jobConfigFactory) {
    this.installationsPath = installationsPath;
    this.jobConfigFactory = jobConfigFactory;
  }

  @Override
  public boolean isInstalled(JobInstance jobInstance) {
    return getAllInstalledJobs().containsKey(jobInstance);
  }

  @Override
  public Map<JobInstance, InstallationRecord> getAllInstalledJobs() {
    Map<JobInstance, InstallationRecord> installations = new HashMap<>();
    for (File jobInstallPath : new File(installationsPath).listFiles()) {
      if (!jobInstallPath.isDirectory()) {
        continue;
      }

      findJobInstances(jobInstallPath, installations);
    }
    return installations;
  }

  /**
   * Finds all the job instances in the specified path and adds a corresponding {@link JobInstance} and
   * {@link InstallationRecord} for each instance.
   *
   * @param jobInstallPath  the path to search for job instances.
   * @param jobs            the map to which the job instances will be added.
   */
  private void findJobInstances(final File jobInstallPath, final Map<JobInstance, InstallationRecord> jobs) {
    try {
      String jobInstallCanonPath = jobInstallPath.getCanonicalPath();
      File configPath = Paths.get(jobInstallCanonPath, CFG_SUBPATH).toFile();
      if (!(configPath.exists() && configPath.isDirectory())) {
        log.debug("Config path not found: " + configPath);
        return;
      }

      for (File configFile : configPath.listFiles()) {

        if (configFile.isFile()) {

          String configFilePath = configFile.getCanonicalPath();
          Config config = jobConfigFactory.getConfig(new URI("file://" + configFilePath));

          if (config.containsKey(JobConfig.JOB_NAME()) && config.containsKey(JobConfig.STREAM_JOB_FACTORY_CLASS())) {

            String jobName = config.get(JobConfig.JOB_NAME());
            String jobId = config.get(JobConfig.JOB_ID(), "1");
            JobInstance jobInstance = new JobInstance(jobName, jobId);

            if (jobs.containsKey(jobInstance)) {
              throw new IllegalStateException(
                  String.format("Found more than one job config with jobName:%s and jobId:%s", jobName, jobId));
            }
            InstallationRecord jobInstall =
                new InstallationRecord(jobName, jobId, jobInstallCanonPath, configFilePath, getBinPath(jobInstallCanonPath));
            jobs.put(jobInstance, jobInstall);
          }
        }
      }
    } catch (Exception e) {
      throw new SamzaException("Exception finding job instance in path: " + jobInstallPath, e);
    }
  }

  /**
   * @param jobInstallPath  the root path of the job installation.
   * @return                the bin directory within the job installation.
   */
  private String getBinPath(String jobInstallPath) {
    return Paths.get(jobInstallPath, BIN_SUBPATH).toString();
  }
}
