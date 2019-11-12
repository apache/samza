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
package org.apache.samza.clustermanager.container.placements;


/**
 * Encapsulates state related to progress of a control action which should be passed between the
 * {@link org.apache.samza.clustermanager.ContainerManager} and any external controllers for reporting state of an action
 */
public class ContainerPlacementStatus {
  public enum StatusCode {
    CREATED, BAD_REQUEST, ACCEPTED, IN_PROGRESS, SUCCEEDED, FAILED
  }

  public String responseMessage;
  public StatusCode status;

  public ContainerPlacementStatus(StatusCode status, String responseMessage) {
    this.responseMessage = responseMessage;
    this.status = status;
  }

  public ContainerPlacementStatus(StatusCode status) {
    this.status = status;
  }

  @Override
  public String toString() {
    return "ControlActionStatus{" + "responseMessage='" + responseMessage + '\'' + ", status=" + status + '}';
  }
}
