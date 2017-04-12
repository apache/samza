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
package org.apache.samza.monitor;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.samza.SamzaException;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.rest.resources.ResourceConstants;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a helper class to interact with the samza-rest apis.
 * It contains the functionality to read the tasks associated with a samza job, to get the status of a samza job.
 */
public class JobsClient {

  private static final Logger LOG = LoggerFactory.getLogger(JobsClient.class);

  private final HttpClient httpClient;

  // list of jobStatusServers that will be used, where each jobStatusServer is of the form Host:Port
  private final List<String> jobStatusServers;

  /**
   * @param jobStatusServers list of jobStatusServers, where each jobStatusServer is of the form Host:Port
   */
  public JobsClient(List<String> jobStatusServers) {
    Preconditions.checkState(!jobStatusServers.isEmpty(), "Job status servers cannot be empty.");
    this.jobStatusServers = new ArrayList<>(jobStatusServers);
    this.httpClient = new HttpClient();
  }

  /**
   * This method retrieves and returns the list of tasks that are associated with a JobInstance.
   * @param jobInstance an instance of the samza job.
   * @return the list of tasks that are associated with the samza job.
   * @throws SamzaException if there were any problems with the http request.
   */
  public List<Task> getTasks(JobInstance jobInstance) {
    return retriableHttpGet(baseUrl -> String.format(ResourceConstants.GET_TASKS_URL, baseUrl,
        jobInstance.getJobName(), jobInstance.getJobId()), new TypeReference<List<Task>>(){});
  }

  /**
   * This method should be used to find the JobStatus of a jobInstance.
   * @param jobInstance a instance of the job.
   * @return the job status of the {@link JobInstance}.
   * @throws SamzaException if there are any problems with the http request.
   */
  public JobStatus getJobStatus(JobInstance jobInstance) {
    Job job = retriableHttpGet(baseUrl -> String.format(ResourceConstants.GET_JOBS_URL, baseUrl,
        jobInstance.getJobName(), jobInstance.getJobId()), new TypeReference<Job>(){});
    return job.getStatus();
  }

  /**
   *
   * This method initiates http get request to the job status servers sequentially,
   * returns the first response from an job status server that returns a 2xx code(success response).
   * When a job status server is down or returns a error response, it tries to reach out to
   * the next job status server in the sequence, to complete the http get request.
   *
   * @param requestUrlBuilder to build the request url, given job status server base url.
   * @param <T> return type of the http get response.
   * @return the response from any one of the job status server.
   * @throws Exception when all the job status servers are unavailable.
   *
   */
  private <T> T retriableHttpGet(Function<String, String> requestUrlBuilder, TypeReference<T> typeReference) {
    Exception fetchException = null;
    for (String jobStatusServer : jobStatusServers) {
      String requestUrl = requestUrlBuilder.apply(jobStatusServer);
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(httpGet(requestUrl), typeReference);
      } catch (Exception e) {
        LOG.error(String.format("Exception when fetching tasks from the url : %s", requestUrl), e);
        fetchException = e;
      }
    }
    throw new SamzaException(String.format("Exception during http get from urls : %s", jobStatusServers), fetchException);
  }

  /**
   * This method initiates http get request on the request url and returns the
   * response returned from the http get.
   * @param requestUrl url on which the http get request has to be performed.
   * @return the http get response.
   * @throws IOException if there are problems with the http get request.
   */
  private byte[] httpGet(String requestUrl) throws IOException {
    GetMethod getMethod = new GetMethod(requestUrl);
    try {
      int responseCode = httpClient.executeMethod(getMethod);
      LOG.debug("Received response code {} for the get request on the url : {}", responseCode, requestUrl);
      byte[] response = getMethod.getResponseBody();
      if (responseCode != 200) {
        throw new SamzaException(String.format("Received response code: %s for get request on: %s, with message: %s.",
                                               responseCode, requestUrl, StringUtils.newStringUtf8(response)));
      }
      return response;
    } finally {
      getMethod.releaseConnection();
    }
  }
}
