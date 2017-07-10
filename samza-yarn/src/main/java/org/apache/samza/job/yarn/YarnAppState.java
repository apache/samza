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
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.yarn.YarnContainer;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * YarnAppState encapsulates Yarn specific state variables that are Yarn specific. This class
 * is useful for information to display in the UI.
 *
 * TODO: make these variables private, provide thread-safe accessors.
 * Saving making changes to variables in YarnAppState because it is used by the UI, and changes to
 * variable names, data structure etc. will require changes to the UI scaml templates too. This is tracked
 * as a part of SAMZA-902
 */

public class YarnAppState {

  /**
   /**
  * State indicating whether the job is healthy or not
  * Modified by both the AMRMCallbackThread and the ContainerAllocator thread
  */

  public Map<String, YarnContainer> runningYarnContainers = new ConcurrentHashMap<>()  ;

  public ConcurrentMap<String, ContainerStatus> failedContainersStatus = new ConcurrentHashMap<>();

  public YarnAppState(int taskId,
                      ContainerId amContainerId,
                      String nodeHost,
                      int nodePort,
                      int nodeHttpPort) {
    this.taskId = taskId;
    this.amContainerId = amContainerId;
    this.nodeHost = nodeHost;
    this.nodePort = nodePort;
    this.nodeHttpPort = nodeHttpPort;
    this.appAttemptId = amContainerId.getApplicationAttemptId();
  }


  @Override
  public String toString() {
    return "YarnAppState{" +
        ", taskId=" + taskId +
        ", amContainerId=" + amContainerId +
        ", nodeHost='" + nodeHost + '\'' +
        ", nodePort=" + nodePort +
        ", nodeHttpPort=" + nodeHttpPort +
        ", appAttemptId=" + appAttemptId +
        ", coordinatorUrl=" + coordinatorUrl +
        ", rpcUrl=" + rpcUrl +
        ", trackingUrl=" + trackingUrl +
        ", runningYarnContainers=" + runningYarnContainers +
        ", failedContainersStatus=" + failedContainersStatus +
        '}';
  }

  /* The following state variables are primarily used for reference in the AM web services   */

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

  //TODO: Make the below 3 variables immutable. Tracked as a part of SAMZA-902. Save for later.
  /**
   * Job Coordinator URL
   * Usage in {@link org.apache.samza.job.yarn.SamzaYarnAppMasterService} &amp; YarnContainerRunner
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
}