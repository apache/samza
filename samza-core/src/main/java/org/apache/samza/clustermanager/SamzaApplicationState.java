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

import org.apache.samza.coordinator.JobModelManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SamzaAppState encapsulates state like - completedProcessors, runningProcessors. This
 * class is also used to display information in the Samza UI. Changing any variable name/
 * data structure type in this class WILL break the UI.
 *
 * TODO:
 * 1.Make these variables private, final
 * 2.Provide thread-safe accessors.
 * //Since the scope of that change is larger, I'm tracking it to work later as a part of SAMZA-902
 *
 */

public class SamzaApplicationState {

  public enum SamzaAppStatus { UNDEFINED, SUCCEEDED, FAILED }

  /**
   * {@link JobModelManager} object associated with this {@link SamzaApplicationState}
   */
  public final JobModelManager jobModelManager;

  /**
   * JMX Server URL, if enabled
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   */
  public String jmxUrl = "";
  /**
   * JMX Server Tunneling URL, if enabled
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   */
  public String jmxTunnelingUrl = "";
  /**
   * The following state variables are required for the correct functioning of the TaskManager
   * Some of them are shared between the AMRMCallbackThread and the ContainerAllocator thread, as mentioned below.
   */

  /**
   * Number of Samza processors that have completed their execution and exited successfully
   */
  public final AtomicInteger completedProcessors = new AtomicInteger(0);

  /**
   * Number of failed containers
   * */
  public final AtomicInteger failedContainers = new AtomicInteger(0);

  /**
   * Number of containers released due to extra allocation returned by the RM
   */
  public final AtomicInteger releasedContainers = new AtomicInteger(0);

  /**
   * Number of processors configured for the job
   */
  public final AtomicInteger processorCount = new AtomicInteger(0);

  /**
   * Set of finished containers
   */
  public final AtomicInteger finishedProcessors = new AtomicInteger(0);

  /**
   *  Number of processors needed for the job to be declared healthy
   *  Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public final AtomicInteger neededProcessors = new AtomicInteger(0);

  /**
   *  Map of the Samza processor ID to the {@link SamzaResource} on which it is submitted for launch.
   *  Modified by both the NMCallback and the ContainerAllocator thread.
   */
  public final ConcurrentMap<String, SamzaResource> pendingProcessors = new ConcurrentHashMap<>(0);

  /**
   *  Map of the Samza processor ID to the {@link SamzaResource} on which it is running.
   *  Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public final ConcurrentMap<String, SamzaResource> runningProcessors = new ConcurrentHashMap<>(0);

  /**
   * Map of Samza processor Id (aka logical id) to execution environment container id (aka physical id ex: yarn container id).
   * This map will be used during the start up phase of new AM in AM-HA.
   *
   * This map is populated at startup of ClusterBasedJobCoordinator.
   * It initially holds the processorId to execution id mapping (if any) present in the coordinator stream.
   * This could correspond to processors currently running or from previous attempt or previous deploy.
   * TODO: SAMZA-2607 : remove this map and all its usages.
   */
  public final ConcurrentMap<String, String> processorToExecutionId = new ConcurrentHashMap<>(0);

  /**
   *  Map of the failed Samza processor ID to resource status of the last attempted of the container.
   *  This map is only used when {@link org.apache.samza.config.ClusterManagerConfig#CLUSTER_MANAGER_CONTAINER_FAIL_JOB_AFTER_RETRIES}
   *  is set to false, this map tracks the containers which have exhausted all retires for restart and JobCoordinator is
   *  no longer attempting to restart this container
   *
   *  Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public final ConcurrentHashMap<String, SamzaResourceStatus> failedProcessors = new ConcurrentHashMap<>(0);

  /**
   * Final status of the application. Made to be volatile s.t. changes will be visible in multiple threads.
   */
  public volatile SamzaAppStatus status = SamzaAppStatus.UNDEFINED;

  /**
   * State indicating whether the job is healthy or not
   * Modified by both the callback handler and the ContainerAllocator thread
   */
  public final AtomicBoolean jobHealthy = new AtomicBoolean(true);

  public final AtomicInteger containerRequests = new AtomicInteger(0);

  public final AtomicInteger matchedResourceRequests = new AtomicInteger(0);

  public final AtomicInteger preferredHostRequests = new AtomicInteger(0);

  public final AtomicInteger anyHostRequests = new AtomicInteger(0);

  public final AtomicInteger expiredPreferredHostRequests = new AtomicInteger(0);

  public final AtomicInteger expiredAnyHostRequests = new AtomicInteger(0);

  /**
   * Number of invalid container notifications.
   *
   * A notification is "invalid" if the corresponding container is not currently managed by the
   * {@link ContainerProcessManager}
   */
  public final AtomicInteger redundantNotifications = new AtomicInteger(0);

  /**
   * Number of container allocations from the RM, that did not meet standby container constraints, in which case the
   * existing resource was given back to the RM, and a new ANY-HOST request had to be made.
   */
  public final AtomicInteger failedStandbyAllocations = new AtomicInteger(0);

  /**
   * Number of occurrences in which a failover of an active container was initiated (due to a node failure), in which a
   * running standby container was available for the failover.
   * If two standby containers were used for one failing active, it counts as two.
   */
  public final AtomicInteger failoversToStandby = new AtomicInteger(0);

  /**
   * Number of occurrences in which a failover of an active container was initiated (due to a node failure), in which no
   * running standby container was available for the failover.
  */
  public final AtomicInteger failoversToAnyHost = new AtomicInteger(0);

  /**
   * Number of occurrences of failed container placement actions
   */
  public final AtomicInteger failedContainerPlacementActions = new AtomicInteger(0);

  /**
   * Number of fault domain aware container requests made for a job.
   */
  public final AtomicInteger faultDomainAwareContainerRequests = new AtomicInteger(0);

  /**
   * Number of fault domain aware containers started for a job.
   */
  public final AtomicInteger faultDomainAwareContainersStarted = new AtomicInteger(0);

  /**
   * Number of expired fault domain aware container requests made for a job.
   */
  public final AtomicInteger expiredFaultDomainAwareContainerRequests = new AtomicInteger(0);

  /**
   * Number of failed fault domain aware container allocations for a job.
   */
  public final AtomicInteger failedFaultDomainAwareContainerAllocations = new AtomicInteger(0);

  public SamzaApplicationState(JobModelManager jobModelManager) {
    this.jobModelManager = jobModelManager;
  }
}
