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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.samza.SamzaException;
import org.apache.samza.rest.model.yarn.YarnApplicationInfo;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.resources.JobsResourceConfig;
import org.apache.samza.rest.resources.YarnJobResourceConfig;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of the {@link JobStatusProvider} that retrieves
 * the job status from the YARN REST api.
 */
public class YarnRestJobStatusProvider implements JobStatusProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnRestJobStatusProvider.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String apiEndpoint;
  private final HttpClient httpClient;

  public YarnRestJobStatusProvider(JobsResourceConfig config) {
    YarnJobResourceConfig yarnConfig = new YarnJobResourceConfig(config);
    this.httpClient = new HttpClient();
    OBJECT_MAPPER.configure(DeserializationConfig.Feature.UNWRAP_ROOT_VALUE, true);
    this.apiEndpoint = String.format("http://%s/ws/v1/cluster/apps",
        yarnConfig.getYarnResourceManagerEndpoint());
  }

  @Override
  public void getJobStatuses(Collection<Job> jobs)
      throws IOException, InterruptedException {
    if (jobs == null || jobs.isEmpty()) {
      return;
    }
    try {
      byte[] response = httpGet(apiEndpoint);
      YarnApplicationInfo yarnApplicationInfo = OBJECT_MAPPER.readValue(response, YarnApplicationInfo.class);
      Map<String, YarnApplicationInfo.YarnApplication> yarnApplications = yarnApplicationInfo.getApplications();
      for (Job job: jobs) {
        String qualifiedJobName = YarnApplicationInfo.getQualifiedJobName(new JobInstance(job.getJobName(), job.getJobId()));
        YarnApplicationInfo.YarnApplication yarnApp = yarnApplications.get(qualifiedJobName);
        if (yarnApp == null) {
          job.setStatusDetail(JobStatus.UNKNOWN.toString());
          job.setStatus(JobStatus.UNKNOWN);
          continue;
        }
        JobStatus samzaStatus = yarnStateToSamzaStatus(YarnApplicationState.valueOf(yarnApp.getState().toUpperCase()));
        if (job.getStatusDetail() == null || samzaStatus != JobStatus.STOPPED) {
          job.setStatusDetail(yarnApp.getState());
          job.setStatus(samzaStatus);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to retrieve node info.", e);
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
   * @return the corresponding Samza job status.
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

  /**
   * Issues a HTTP Get request to the provided url and returns the response
   * @param requestUrl the request url
   * @return the response
   * @throws IOException if there are problems with the http get request.
   */
  private byte[] httpGet(String requestUrl)
      throws IOException {
    GetMethod getMethod = new GetMethod(requestUrl);
    try {
      int responseCode = this.httpClient.executeMethod(getMethod);
      LOGGER.debug("Received response code: {} for the get request on the url: {}", responseCode, requestUrl);
      byte[] response = getMethod.getResponseBody();
      if (responseCode != HttpStatus.SC_OK) {
        throw new SamzaException(
            String.format("Received response code: %s for get request on: %s, with message: %s.", responseCode,
                requestUrl, StringUtils.newStringUtf8(response)));
      }
      return response;
    } finally {
      getMethod.releaseConnection();
    }
  }
}
