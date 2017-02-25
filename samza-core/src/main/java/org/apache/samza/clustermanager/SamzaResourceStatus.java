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

/**
 * <p><code>SamzaResourceStatus</code> represents the current status of a
 * <code>StreamProcessor</code> and the resource it is on.</p>
 *
 * <p>It provides details such as:
 *   <ul>
 *     <li><code>resourceID</code> of the resource.</li>
 *     <li><em>Exit status</em> of the StreamProcessor.</li>
 *     <li><em>Diagnostic</em> message for a failed/pre-empted StreamProcessor.</li>
 *   </ul>
 *
 *
 * The exact semantics of various exit codes and failure modes are evolving.
 * Currently the following failures are handled -  termination of a process running in the resource,
 * resource preemption, disk failures on host.
 *
 */
public final class SamzaResourceStatus {
  /**
   * Indicates that the StreamProcessor on the resource successfully completed.
   */
  public static final int SUCCESS = 0;
  /**
   * Indicates the failure of the StreamProcessor running on the resource.
   */
  public static final int ABORTED = -100;
  /**
   * Indicates that the resource was preempted (given to another processor) by
   * the cluster manager
   */
  public static final int PREEMPTED = -102;
  /**
   * Indicates a disk failure in the host the resource is on.
   * Currently these are modelled after Yarn, could evolve as we add integrations with
   * many cluster managers.
   */
  public static final int DISK_FAIL = -101;

  private final String resourceID;
  private final String diagnostics;
  private final int exitCode;


  public SamzaResourceStatus(String resourceID, String diagnostics, int exitCode) {
    this.resourceID = resourceID;
    this.diagnostics = diagnostics;
    this.exitCode = exitCode;
  }

  public int getExitCode() {
    return exitCode;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public String getResourceID() {
    return resourceID;
  }

  @Override
  public String toString() {
    return "SamzaResourceStatus{" +
            "resourceID='" + resourceID + '\'' +
            ", diagnostics='" + diagnostics + '\'' +
            ", exitCode=" + exitCode +
            '}';
  }
}
