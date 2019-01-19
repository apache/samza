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
package org.apache.samza.clustermanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.jetty.http.HttpParser.*;


/**
 * This is the allocator thread that will be used by ContainerProcessManager when host-affinity is enabled for a job. It is similar
 * to {@link ContainerAllocator}, except that it considers locality for allocation.
 *
 * In case of host-affinity, each request ({@link SamzaResourceRequest} encapsulates the identifier of the container
 * to be run and a "preferredHost". preferredHost is determined by the locality mappings in the coordinator stream.
 * This thread periodically wakes up and makes the best-effort to assign a container to the preferredHost. If the
 * preferredHost is not returned by the cluster manager before the corresponding container expires, the thread
 * assigns the container to any other host that is allocated next.
 *
 * The container expiry is determined by CONTAINER_REQUEST_TIMEOUT and is configurable on a per-job basis.
 *
 * If there aren't enough containers, it waits by sleeping for allocatorSleepIntervalMs milliseconds.
 */
//This class is used in the refactored code path as called by run-jc.sh

public class HostAwareContainerAllocator extends AbstractContainerAllocator {
  private static final Logger log = LoggerFactory.getLogger(HostAwareContainerAllocator.class);
  /**
   * Tracks the expiration of a request for resources.
   */
  private final int requestTimeout;

  public HostAwareContainerAllocator(ClusterResourceManager manager, int timeout, Config config,
      SamzaApplicationState state) {
    super(manager, new ResourceRequestState(true, manager), config, state);
    this.requestTimeout = timeout;
  }

  /**
   * Since host-affinity is enabled, all allocated resources are buffered in the list keyed by "preferredHost".
   *
   * If the requested host is not available, the thread checks to see if the request has expired.
   * If it has expired, it runs the container with expectedContainerID on one of the available resources from the
   * allocatedContainers buffer keyed by "ANY_HOST".
   */
  @Override
  public void assignResourceRequests() {
    while (hasPendingRequest()) {
      SamzaResourceRequest request = peekPendingRequest();
      log.info("Handling request: " + request.getContainerID() + " " + request.getRequestTimestampMs() + " "
          + request.getPreferredHost());
      String preferredHost = request.getPreferredHost();
      String containerID = request.getContainerID();

      if (hasAllocatedResource(preferredHost)) {
        // Found allocated container at preferredHost
        log.info("Found a matched-container {} on the preferred host. Running on {}", containerID, preferredHost);

        // Try to launch streamProcessor on this preferredHost
        checkStandbyTaskAndRunStreamProcessor(request, preferredHost, peekAllocatedResource(preferredHost), state);
        state.matchedResourceRequests.incrementAndGet();
      } else {
        log.info("Did not find any allocated resources on preferred host {} for running container id {}", preferredHost,
            containerID);

        boolean expired = requestExpired(request);
        boolean resourceAvailableOnAnyHost = hasAllocatedResource(ResourceRequestState.ANY_HOST);

        if (expired) {
          updateExpiryMetrics(request);
          if (resourceAvailableOnAnyHost) {
            log.info("Request for container: {} on {} has expired. Running on ANY_HOST", request.getContainerID(),
                request.getPreferredHost());
            checkStandbyTaskAndRunStreamProcessor(request, ResourceRequestState.ANY_HOST,
                peekAllocatedResource(ResourceRequestState.ANY_HOST), state);
          } else {
            log.info("Request for container: {} on {} has expired. Requesting additional resources on ANY_HOST.",
                request.getContainerID(), request.getPreferredHost());
            resourceRequestState.cancelResourceRequest(request);
            requestResource(containerID, ResourceRequestState.ANY_HOST);
          }
        } else {
          log.info(
              "Request for container: {} on {} has not yet expired. Request creation time: {}. Request timeout: {}",
              new Object[]{request.getContainerID(), request.getPreferredHost(), request.getRequestTimestampMs(),
                  requestTimeout});
          break;
        }
      }
    }
  }

  /**
   * Checks if a request has expired.
   * @param request
   * @return
   */
  private boolean requestExpired(SamzaResourceRequest request) {
    long currTime = System.currentTimeMillis();
    boolean requestExpired = currTime - request.getRequestTimestampMs() > requestTimeout;
    if (requestExpired) {
      log.info("Request {} with currTime {} has expired", request, currTime);
    }
    return requestExpired;
  }

  private void updateExpiryMetrics(SamzaResourceRequest request) {
    String preferredHost = request.getPreferredHost();
    if (ResourceRequestState.ANY_HOST.equals(preferredHost)) {
      state.expiredAnyHostRequests.incrementAndGet();
    } else {
      state.expiredPreferredHostRequests.incrementAndGet();
    }
  }

  private boolean checkStandbyTaskAndRunStreamProcessor(SamzaResourceRequest request, String preferredHost,
      SamzaResource samzaResource, SamzaApplicationState state) {
    String containerID = request.getContainerID();

    if (checkStandbyTaskConstraints(request, samzaResource, state)) {
      // This resource can be used to launch this container
      log.info("Running container {} on preferred host {} meets standby constraints, launching on {}", containerID,
          preferredHost, samzaResource.getHost());
      runStreamProcessor(request, preferredHost);
      return true;
    } else {
      // This resource cannot be used to launch this container, so we make a ANY_HOST request for another container
      log.info(
          "Running container {} on host {} does not meet standby constraints, cancelling resource request and making a new ANY_HOST request",
          containerID, samzaResource.getHost());
      resourceRequestState.cancelResourceRequest(request);
      resourceRequestState.releaseUnstartableContainer(samzaResource);
      requestResource(containerID, ResourceRequestState.ANY_HOST);
      return false;
    }
  }

  // Helper method to check if this SamzaResourceRequest for a container can be met on this resource, given standby
  // container constraints
  private boolean checkStandbyTaskConstraints(SamzaResourceRequest request, SamzaResource samzaResource,
      SamzaApplicationState samzaApplicationState) {

    // if standby tasks are not enabled return true
    if (!new JobConfig(config).getStandbyTasksEnabled()) {
      return true;
    }

    String containerIDForStart = request.getContainerID();
    String host = samzaResource.getHost();

    List<String> containerIDsForStandbyConstraints =
        getContainerIDsToCheckStandbyConstraints(containerIDForStart, samzaApplicationState.jobModelManager.jobModel());

    LOG.info("Container {} has standby task constraints with containers {}", containerIDForStart,
        containerIDsForStandbyConstraints);

    // check if any of these conflicting containers are running/launching on host
    for (String containerID : containerIDsForStandbyConstraints) {

      SamzaResource resource = samzaApplicationState.pendingContainers.get(containerID);

      // return false if a conflicting container is pending for launch on the host
      if (resource != null && resource.getHost().equals(host)) {
        return false;
      }

      // return false if a conflicting container is running on the host
      resource = samzaApplicationState.runningContainers.get(containerID);
      if (resource != null && resource.getHost().equals(host)) {
        return false;
      }
    }

    return true;
  }

  /**
   *  Given a containerID and job model, it returns the containerids of all containers that have
   *  a. standby tasks corresponding to active tasks on the given container,
   *  or
   *  b. have active tasks corresponding to standby tasks on the given container.
   *
   *  This is used to ensure that an active task and all its corresponding standby tasks are on separate hosts.
   */
  public static List<String> getContainerIDsToCheckStandbyConstraints(String containerID, JobModel jobModel) {

    ContainerModel givenContainerModel = jobModel.getContainers().get(containerID);
    List<String> containerIDsWithStandbyConstraints = new ArrayList<>();

    // iterate over all containerModels in the jobModel
    for (ContainerModel containerModel : jobModel.getContainers().values()) {

      // add to list if active and standby tasks on the two containerModels overlap
      if (!givenContainerModel.equals(containerModel) && checkTasksOverlap(givenContainerModel, containerModel)) {
        containerIDsWithStandbyConstraints.add(containerModel.getId());
      }
    }
    return containerIDsWithStandbyConstraints;
  }

  // Helper method that checks if tasks on the two containerModels overlap
  private static boolean checkTasksOverlap(ContainerModel containerModel1, ContainerModel containerModel2) {

    Set<TaskName> activeTasksOnContainer1 = convertAllTasksToActive(containerModel1);
    Set<TaskName> activeTasksOnContainer2 = convertAllTasksToActive(containerModel2);

    return !Collections.disjoint(activeTasksOnContainer1, activeTasksOnContainer2);
  }

  private static Set<TaskName> convertAllTasksToActive(ContainerModel containerModel) {
    Set<TaskName> tasksInActiveMode = getAllTasks(containerModel, TaskMode.Active);

    tasksInActiveMode.addAll(getAllTasks(containerModel, TaskMode.Standby).stream()
        .map(taskName -> getActiveTaskFor(taskName))
        .collect(Collectors.toSet()));

    return tasksInActiveMode;
  }

  //
  // Helper method to getAllTaskModels of this container in the given taskMode
  private static Set<TaskName> getAllTasks(ContainerModel containerModel, TaskMode taskMode) {
    return containerModel.getTasks()
        .values()
        .stream()
        .filter(e -> e.getTaskMode().equals(taskMode))
        .map(taskModel -> taskModel.getTaskName())
        .collect(Collectors.toSet());
  }

  // TODO: convert into util method that relies on taskNames to get active task for a given standby task
  private static TaskName getActiveTaskFor(TaskName standbyTaskName) {
    return new TaskName(standbyTaskName.getTaskName().split("-")[1]);
  }
}
