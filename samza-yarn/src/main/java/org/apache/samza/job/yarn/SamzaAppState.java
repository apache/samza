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

package org.apache.samza.job.yarn;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SamzaAppState {
  /**
   * Job Coordinator is started in the AM and follows the {@link org.apache.samza.job.yarn.SamzaAppMasterService}
   * lifecycle. It helps querying JobModel related info in {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   * and locality information when host-affinity is enabled in {@link org.apache.samza.job.yarn.SamzaTaskManager}
   */
  public final JobCoordinator jobCoordinator;

  /*  The following state variables are primarily used for reference in the AM web services   */
  /**
   * Task Id of the AM
   * Used for displaying in the AM UI. Usage found in {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   * and scalate/WEB-INF/views/index.scaml
   */
  public final int taskId;
  /**
   * Id of the AM container (as allocated by the RM)
   * Used for displaying in the AM UI. Usage in {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   * and scalate/WEB-INF/views/index.scaml
   */
  public final ContainerId amContainerId;
  /**
   * Host name of the NM on which the AM is running
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   */
  public final String nodeHost;
  /**
   * NM port on which the AM is running
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   */
  public final int nodePort;
  /**
   * Http port of the NM on which the AM is running
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   */
  public final int nodeHttpPort;
  /**
   * Application Attempt Id as provided by Yarn
   * Used for displaying in the AM UI. See scalate/WEB-INF/views/index.scaml
   * and {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   */
  public final ApplicationAttemptId appAttemptId;
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
   * Job Coordinator URL
   * Usage in {@link org.apache.samza.job.yarn.SamzaAppMasterService} &amp; ContainerUtil
   */
  public URL coordinatorUrl = null;
  /**
   * URL of the {@link org.apache.samza.webapp.ApplicationMasterRestServlet}
   */
  public URL rpcUrl = null;
  /**
   * URL of the {@link org.apache.samza.webapp.ApplicationMasterWebServlet}
   */
  public URL trackingUrl = null;

  /**
   * The following state variables are required for the correct functioning of the TaskManager
   * Some of them are shared between the AMRMCallbackThread and the ContainerAllocator thread, as mentioned below.
   */

  /**
   * Number of containers that have completed their execution and exited successfully
   */
  public AtomicInteger completedContainers = new AtomicInteger(0);

  /**
   * Number of failed containers
   * */
  public AtomicInteger failedContainers = new AtomicInteger(0);

  /**
   * Number of containers released due to extra allocation returned by the RM
   */
  public AtomicInteger releasedContainers = new AtomicInteger(0);

  /**
   * ContainerStatus of failed containers.
   */
  public ConcurrentMap<String, ContainerStatus> failedContainersStatus = new ConcurrentHashMap<String, ContainerStatus>();

  /**
   * Number of containers configured for the job
   */
  public int containerCount = 0;

  /**
   * Set of finished containers - TODO: Can be changed to a counter
   */
  public Set<Integer> finishedContainers = new HashSet<Integer>();

  /**
   *  Number of containers needed for the job to be declared healthy
   *  Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public AtomicInteger neededContainers = new AtomicInteger(0);

  /**
   *  Map of the samzaContainerId to the {@link org.apache.samza.job.yarn.YarnContainer} on which it is running
   *  Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public ConcurrentMap<Integer, YarnContainer> runningContainers = new ConcurrentHashMap<Integer, YarnContainer>(0);

  /**
   * Final status of the application
   * Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public FinalApplicationStatus status = FinalApplicationStatus.UNDEFINED;

  /**
   * State indicating whether the job is healthy or not
   * Modified by both the AMRMCallbackThread and the ContainerAllocator thread
   */
  public AtomicBoolean jobHealthy = new AtomicBoolean(true);

  public AtomicInteger containerRequests = new AtomicInteger(0);

  public AtomicInteger matchedContainerRequests = new AtomicInteger(0);

  public SamzaAppState(JobCoordinator jobCoordinator,
                       int taskId,
                       ContainerId amContainerId,
                       String nodeHost,
                       int nodePort,
                       int nodeHttpPort) {
    this.jobCoordinator = jobCoordinator;
    this.taskId = taskId;
    this.amContainerId = amContainerId;
    this.nodeHost = nodeHost;
    this.nodePort = nodePort;
    this.nodeHttpPort = nodeHttpPort;
    this.appAttemptId = amContainerId.getApplicationAttemptId();

  }
}
