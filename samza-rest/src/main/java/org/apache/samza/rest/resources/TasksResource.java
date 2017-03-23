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

import com.google.common.base.Preconditions;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.rest.proxy.task.TaskProxyFactory;
import org.apache.samza.rest.proxy.task.SamzaTaskProxy;
import org.apache.samza.rest.proxy.task.TaskProxy;
import org.apache.samza.rest.proxy.task.TaskResourceConfig;
import org.apache.samza.util.ClassLoaderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The REST resource for tasks. Handles the requests that are at the tasks scope.
 */
@Singleton
@Path("/v1/jobs")
public class TasksResource {

  private static final Logger LOG = LoggerFactory.getLogger(TasksResource.class);

  private final TaskProxy taskProxy;

  /**
   * Initializes a TaskResource with {@link TaskProxy} from the
   * {@link TaskProxyFactory} class specified in the configuration.
   *
   * @param config  the configuration containing the {@link TaskProxyFactory} class.
   */
  public TasksResource(TaskResourceConfig config) {
    String taskProxyFactory = config.getTaskProxyFactory();
    Preconditions.checkArgument(StringUtils.isNotEmpty(taskProxyFactory),
                                String.format("Missing config: %s", TaskResourceConfig.CONFIG_TASK_PROXY_FACTORY));
    try {
      TaskProxyFactory factory = ClassLoaderHelper.fromClassName(taskProxyFactory);
      taskProxy = factory.getTaskProxy(config);
    } catch (Exception e) {
      LOG.error(String.format("Exception in building TasksResource with config: %s.", config), e);
      throw new SamzaException(e);
    }
  }

  /**
   * Gets the list of {@link Task} for the job instance specified by jobName and jobId.
   * @param jobName the name of the job as configured in {@link org.apache.samza.config.JobConfig#JOB_NAME}
   * @param jobId the id of the job as configured in {@link org.apache.samza.config.JobConfig#JOB_ID}.
   * @return a {@link javax.ws.rs.core.Response.Status#OK} {@link javax.ws.rs.core.Response}
   *         contains a list of {@link Task}, where each task belongs to
   *         the samza job. {@link javax.ws.rs.core.Response.Status#BAD_REQUEST} is returned for invalid
   *         job instances.
   */
  @GET
  @Path("/{jobName}/{jobId}/tasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTasks(
      @PathParam("jobName") final String jobName,
      @PathParam("jobId") final String jobId) {
    try {
      return Response.ok(taskProxy.getTasks(new JobInstance(jobName, jobId))).build();
    } catch (IllegalArgumentException e) {
      String message = String.format("Invalid arguments for getTasks. jobName: %s, jobId: %s.", jobName, jobId);
      LOG.error(message, e);
      return Responses.badRequestResponse(message);
    } catch (Exception e) {
      LOG.error(String.format("Error in getTasks with arguments jobName: %s, jobId: %s.", jobName, jobId), e);
      return Responses.errorResponse(e.getMessage());
    }
  }
}
