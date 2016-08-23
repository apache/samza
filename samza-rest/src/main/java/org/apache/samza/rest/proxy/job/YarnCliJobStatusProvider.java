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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.samza.SamzaException;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.script.ScriptOutputHandler;
import org.apache.samza.rest.script.ScriptPathProvider;
import org.apache.samza.rest.script.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of the {@link JobStatusProvider} that retrieves
 * the job status from the YARN command line interface.
 */
public class YarnCliJobStatusProvider implements JobStatusProvider {
  private static final Logger log = LoggerFactory.getLogger(YarnCliJobStatusProvider.class);
  private static final String JOB_NAME_ID_FORMAT = "%s_%s";
  private final ScriptPathProvider scriptPathProvider;

  /**
   * Constructs the job name used in YARN. This is the value shown in the "Name"
   * column of the Resource Manager UI.
   *
   * @param jobInstance the instance of the job.
   * @return            the job name to use for the job in YARN.
   */
  public static String getQualifiedJobName(JobInstance jobInstance) {
    return String.format(JOB_NAME_ID_FORMAT, jobInstance.getJobName(), jobInstance.getJobId());
  }

  /**
   * Default constructor.
   *
   * @param provider a delegate that provides the path to the Samza yarn scripts.
   */
  public YarnCliJobStatusProvider(ScriptPathProvider provider) {
    scriptPathProvider = provider;
  }

  @Override
  public void getJobStatuses(Collection<Job> jobs)
      throws IOException, InterruptedException {
    if (jobs == null || jobs.isEmpty()) {
      return;
    }

    // If the scripts are in the jobs, they will be in all job installations, so just pick one and get the script path.
    Job anyJob = jobs.iterator().next();
    String scriptPath = scriptPathProvider.getScriptPath(new JobInstance(anyJob.getJobName(), anyJob.getJobId()), "run-class.sh");

    // We will identify jobs returned by the YARN application states by their qualified names, so build a map
    // to translate back from that name to the JobInfo we wish to populate. This avoids parsing/delimiter issues.
    final Map<String, Job> qualifiedJobToInfo = new HashMap<>();
    for(Job job : jobs) {
      qualifiedJobToInfo.put(getQualifiedJobName(new JobInstance(job.getJobName(), job.getJobId())), job);
    }

    // Run "application -list" command and get the YARN state for each application
    ScriptRunner runner = new ScriptRunner();
    int resultCode = runner.runScript(scriptPath, new ScriptOutputHandler() {
      @Override
      public void processScriptOutput(InputStream output)
          throws IOException {
        InputStreamReader isr = new InputStreamReader(output);
        BufferedReader br = new BufferedReader(isr);
        String line;
        String APPLICATION_PREFIX = "application_";
        log.debug("YARN status:");
        while ((line = br.readLine()) != null) {
          log.debug(line);
          if (line.startsWith(APPLICATION_PREFIX)) {
            String[] columns = line.split("\\s+");
            String qualifiedName = columns[1];
            String yarnState = columns[5];

            JobStatus samzaStatus = yarnStateToSamzaStatus(YarnApplicationState.valueOf(yarnState.toUpperCase()));
            Job job = qualifiedJobToInfo.get(qualifiedName);

            // If job is null, it wasn't requested.  The default status is STOPPED because there could be many
            // application attempts in that status. Only update the job status if it's not STOPPED.
            if (job != null && (job.getStatusDetail() == null || samzaStatus != JobStatus.STOPPED)) {
              job.setStatusDetail(yarnState);
              job.setStatus(samzaStatus);
            }
          }
        }
      }
    }, "org.apache.hadoop.yarn.client.cli.ApplicationCLI", "application", "-list", "-appStates", "ALL");

    if (resultCode != 0) {
      throw new SamzaException("Failed to get job status. Result code: " + resultCode);
    }
  }

  @Override
  public Job getJobStatus(JobInstance jobInstance)
      throws IOException, InterruptedException {
    Job info = new Job(jobInstance.getJobName(), jobInstance.getJobId());
    getJobStatuses(Collections.singletonList(info));
    return info;
  }

  /**
   * Translates the YARN application state to the more generic Samza job status.
   *
   * @param yarnState the YARN application state to translate.
   * @return          the corresponding Samza job status.
   */
  private JobStatus yarnStateToSamzaStatus(YarnApplicationState yarnState) {
    switch (yarnState) {
      case RUNNING:
        return JobStatus.STARTED;
      case NEW:
      case NEW_SAVING:
      case SUBMITTED:
      case ACCEPTED:
        return JobStatus.STARTING;
      case FINISHED:
      case FAILED:
      case KILLED:
      default:
        return JobStatus.STOPPED;
    }
  }
}
