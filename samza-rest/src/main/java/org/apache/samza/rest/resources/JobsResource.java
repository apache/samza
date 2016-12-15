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

import java.util.Collections;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.proxy.job.AbstractJobProxy;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.rest.proxy.job.JobProxy;
import org.apache.samza.rest.proxy.job.JobProxyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The REST resource for jobs. Handles all requests for the jobs collection
 * or individual job instances.
 */
@Singleton
@Path("/v1/jobs")
public class JobsResource {
  private static final Logger log = LoggerFactory.getLogger(JobsResource.class);

  /** The primary interface for interacting with jobs. */
  private final JobProxy jobProxy;

  /**
   * Initializes a JobResource with {@link JobProxy} from the
   * {@link JobProxyFactory} class specified in the configuration.
   *
   * @param config  the configuration containing the {@link JobProxyFactory} class.
   */
  public JobsResource(JobsResourceConfig config) {
    jobProxy = AbstractJobProxy.fromFactory(config);
  }

  /**
   * Gets the {@link Job} for all the jobs installed on this host.
   *
   * @return a {@link javax.ws.rs.core.Response.Status#OK} {@link javax.ws.rs.core.Response} containing a list of
   * {@link Job} for all the installed Samza jobs installed on this host.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getInstalledJobs() {
    try {
      return Response.ok(jobProxy.getAllJobStatuses()).build();
    } catch (Exception e) {
      log.error("Error in getInstalledJobs.", e);
      return errorResponse(e.getMessage());
    }
  }

  /**
   * Gets the {@link Job} for the job instance specified by jobName and jobId if
   * it is installed on this host.
   *
   * @param jobName the name of the job as configured in {@link org.apache.samza.config.JobConfig#JOB_NAME}.
   * @param jobId   the id of the job as configured in {@link org.apache.samza.config.JobConfig#JOB_ID}.
   * @return        a {@link javax.ws.rs.core.Response.Status#OK} {@link javax.ws.rs.core.Response}
   *                containing a {@link Job} for the Samza job if it is
   *                installed on this host. {@link javax.ws.rs.core.Response.Status#NOT_FOUND} and
   *                {@link javax.ws.rs.core.Response.Status#INTERNAL_SERVER_ERROR} can occur for corresponding errors.
   */
  @GET
  @Path("/{jobName}/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getJob(
      @PathParam("jobName") final String jobName,
      @PathParam("jobId") final String jobId) {
    JobInstance jobInstance = new JobInstance(jobName, jobId);
    try {
      if (!jobProxy.jobExists(jobInstance)) {
        return Response.status(Response.Status.NOT_FOUND).entity(Collections.singletonMap("message",
            String.format("%s does not exist.", jobInstance))).build();
      }

      Job job = jobProxy.getJobStatus(jobInstance);
      return Response.ok(job).build();
    } catch (Exception e) {
      log.error("Error in getJob.", e);
      return errorResponse(e.getMessage());
    }
  }

  /**
   *
   * @param jobName the name of the job as configured in {@link org.apache.samza.config.JobConfig#JOB_NAME}.
   * @param jobId   the id of the job as configured in {@link org.apache.samza.config.JobConfig#JOB_ID}.
   * @param status   the {@link JobStatus} to which the job will transition.
   * @return        a {@link javax.ws.rs.core.Response.Status#ACCEPTED} {@link javax.ws.rs.core.Response}
   *                containing a {@link Job} for the Samza job if it is
   *                installed on this host. {@link javax.ws.rs.core.Response.Status#NOT_FOUND}
   *                {@link javax.ws.rs.core.Response.Status#BAD_REQUEST} and
   *                {@link javax.ws.rs.core.Response.Status#INTERNAL_SERVER_ERROR} can occur for corresponding errors.
   */
  @PUT
  @Path("/{jobName}/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateJobStatus(
      @PathParam("jobName") final String jobName,
      @PathParam("jobId") final String jobId,
      @QueryParam("status") String status) {
    JobInstance jobInstance = new JobInstance(jobName, jobId);
    try {
      if (!jobProxy.jobExists(jobInstance)) {
        return Response.status(Response.Status.NOT_FOUND).entity(Collections
            .singletonMap("message", String.format("Job %s instance %s is not installed on this host.", jobName, jobId))).build();
      }

      if (status == null) {
        throw new IllegalArgumentException("Unrecognized status parameter: " + status);
      }

      JobStatus samzaStatus = JobStatus.valueOf(status.toUpperCase());
      switch (samzaStatus) {
        case STARTED:
          log.info("Starting {}", jobInstance);
          jobProxy.start(jobInstance);
          Job infoStarted = jobProxy.getJobStatus(jobInstance);
          return Response.accepted(infoStarted).build();
        case STOPPED:
          log.info("Stopping {}", jobInstance);
          jobProxy.stop(jobInstance);
          Job infoStopped = jobProxy.getJobStatus(jobInstance);
          return Response.accepted(infoStopped).build();
        default:
          throw new IllegalArgumentException("Unsupported status: " + status);
      }
    } catch (IllegalArgumentException e) {
      log.info(String.format("Illegal arguments updateJobStatus. JobName:%s JobId:%s Status=%s", jobName, jobId, status), e);
      return Response.status(Response.Status.BAD_REQUEST).entity(
          Collections.singletonMap("message", e.getMessage())).build();
    } catch (Exception e) {
      log.error("Error in updateJobStatus.", e);
      return errorResponse(String.format("Error type: %s message: %s", e.toString(), e.getMessage()));
    }
  }

  /**
   * Constructs a consistent format for error responses. This method should be used for every error case.
   *
   * @param message the error message to report.
   * @return        the {@link Response} containing the error message.
   */
  private Response errorResponse(String message) {
    return Response.serverError().entity(Collections.singletonMap("message", message)).build();
  }
}
