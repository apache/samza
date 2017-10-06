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
package org.apache.samza.rest.model;

import java.util.Objects;
import org.codehaus.jackson.annotate.JsonProperty;


public class ApplicationInfo {
  @JsonProperty("finishTime")
  private long _finishTime;

  @JsonProperty("amContainerLogs")
  private String _amContainerLogs;

  @JsonProperty("trackingUI")
  private String _trackingHistory;

  @JsonProperty("state")
  private String _state;

  @JsonProperty("user")
  private String _user;

  @JsonProperty("id")
  private String _id;

  @JsonProperty("clusterId")
  private String _clusterId;

  @JsonProperty("finalStatus")
  private String _finalStatus;

  @JsonProperty("amHostHttpAddress")
  private String _amHostHttpAddress;

  @JsonProperty("applicationType")
  private String _applicationType;

  @JsonProperty("applicationTags")
  private String _applicationTags;

  @JsonProperty("finishedTime")
  private String _finishedTime;

  @JsonProperty("preemptedResourceMB")
  private String _preemptedResourceMB;

  @JsonProperty("preemptedResourceVCores")
  private String _preemptedResourceVCores;

  @JsonProperty("numNonAMContainerPreempted")
  private String _numNonAMContainerPreempted;

  @JsonProperty("numAMContainerPreempted")
  private String _numAMContainerPreempted;

  @JsonProperty("progress")
  private int _progress;

  @JsonProperty("name")
  private String _name;

  @JsonProperty("startedTime")
  private long _startedTime;

  @JsonProperty("elapsedTime")
  private long _elapsedTime;

  @JsonProperty("diagnostics")
  private String _diagnostics;

  @JsonProperty("trackingUrl")
  private String _trackingUrl;

  @JsonProperty("queue")
  private String _queue;

  @JsonProperty("allocatedMB")
  private int _allocatedMB;

  @JsonProperty("allocatedVCores")
  private int _allocatedVCores;

  @JsonProperty("runningContainers")
  private int _runningContainers;

  @JsonProperty("memorySeconds")
  private int _memorySeconds;

  @JsonProperty("vcoreSeconds")
  private int _vcoreSeconds;

  ApplicationInfo() {

  }

  public ApplicationInfo(long finishTime, String amContainerLogs, String trackingHistory, String state, String user,
      String id, String clusterId, String finalStatus, String amHostHttpAddress, String applicationType,
      String applicationTags, String finishedTime, String preemptedResourceMB, String preemptedResourceVCores,
      String numNonAMContainerPreempted, String numAMContainerPreempted, int progress, String name, long startedTime,
      long elapsedTime, String diagnostics, String trackingUrl, String queue, int allocatedMB, int allocatedVCores,
      int runningContainers, int memorySeconds, int vcoreSeconds) {
    _finishTime = finishTime;
    _amContainerLogs = amContainerLogs;
    _trackingHistory = trackingHistory;
    _state = state;
    _user = user;
    _id = id;
    _clusterId = clusterId;
    _finalStatus = finalStatus;
    _amHostHttpAddress = amHostHttpAddress;
    _applicationType = applicationType;
    _applicationTags = applicationTags;
    _finishedTime = finishedTime;
    _preemptedResourceMB = preemptedResourceMB;
    _preemptedResourceVCores = preemptedResourceVCores;
    _numNonAMContainerPreempted = numNonAMContainerPreempted;
    _numAMContainerPreempted = numAMContainerPreempted;
    _progress = progress;
    _name = name;
    _startedTime = startedTime;
    _elapsedTime = elapsedTime;
    _diagnostics = diagnostics;
    _trackingUrl = trackingUrl;
    _queue = queue;
    _allocatedMB = allocatedMB;
    _allocatedVCores = allocatedVCores;
    _runningContainers = runningContainers;
    _memorySeconds = memorySeconds;
    _vcoreSeconds = vcoreSeconds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApplicationInfo that = (ApplicationInfo) o;
    return _finishTime == that._finishTime && _progress == that._progress && _startedTime == that._startedTime
        && _elapsedTime == that._elapsedTime && _allocatedMB == that._allocatedMB
        && _allocatedVCores == that._allocatedVCores && _runningContainers == that._runningContainers
        && _memorySeconds == that._memorySeconds && _vcoreSeconds == that._vcoreSeconds && Objects.equals(
        _amContainerLogs, that._amContainerLogs) && Objects.equals(_trackingHistory, that._trackingHistory)
        && Objects.equals(_state, that._state) && Objects.equals(_user, that._user) && Objects.equals(_id, that._id)
        && Objects.equals(_clusterId, that._clusterId) && Objects.equals(_finalStatus, that._finalStatus)
        && Objects.equals(_amHostHttpAddress, that._amHostHttpAddress) && Objects.equals(_applicationType,
        that._applicationType) && Objects.equals(_applicationTags, that._applicationTags) && Objects.equals(
        _finishedTime, that._finishedTime) && Objects.equals(_preemptedResourceMB, that._preemptedResourceMB) && Objects
        .equals(_preemptedResourceVCores, that._preemptedResourceVCores) && Objects.equals(_numNonAMContainerPreempted,
        that._numNonAMContainerPreempted) && Objects.equals(_numAMContainerPreempted, that._numAMContainerPreempted)
        && Objects.equals(_name, that._name) && Objects.equals(_diagnostics, that._diagnostics) && Objects.equals(
        _trackingUrl, that._trackingUrl) && Objects.equals(_queue, that._queue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_finishTime, _amContainerLogs, _trackingHistory, _state, _user, _id, _clusterId, _finalStatus,
        _amHostHttpAddress, _applicationType, _applicationTags, _finishedTime, _preemptedResourceMB,
        _preemptedResourceVCores, _numNonAMContainerPreempted, _numAMContainerPreempted, _progress, _name, _startedTime,
        _elapsedTime, _diagnostics, _trackingUrl, _queue, _allocatedMB, _allocatedVCores, _runningContainers,
        _memorySeconds, _vcoreSeconds);
  }

  public long getFinishTime() {
    return _finishTime;
  }

  public String getAmContainerLogs() {
    return _amContainerLogs;
  }

  public String getTrackingHistory() {
    return _trackingHistory;
  }

  public String getState() {
    return _state;
  }

  public String getUser() {
    return _user;
  }

  public String getId() {
    return _id;
  }

  public String getClusterId() {
    return _clusterId;
  }

  public String getFinalStatus() {
    return _finalStatus;
  }

  public String getAmHostHttpAddress() {
    return _amHostHttpAddress;
  }

  public int getProgress() {
    return _progress;
  }

  public String getName() {
    return _name;
  }

  public long getStartedTime() {
    return _startedTime;
  }

  public long getElapsedTime() {
    return _elapsedTime;
  }

  public String getDiagnostics() {
    return _diagnostics;
  }

  public String getTrackingUrl() {
    return _trackingUrl;
  }

  public String getQueue() {
    return _queue;
  }

  public int getAllocatedMB() {
    return _allocatedMB;
  }

  public int getAllocatedVCores() {
    return _allocatedVCores;
  }

  public int getRunningContainers() {
    return _runningContainers;
  }

  public int getMemorySeconds() {
    return _memorySeconds;
  }

  public int getVcoreSeconds() {
    return _vcoreSeconds;
  }

  public String getApplicationType() {
    return _applicationType;
  }

  public String getApplicationTags() {
    return _applicationTags;
  }

  public String getFinishedTime() {
    return _finishedTime;
  }

  public String getPreemptedResourceMB() {
    return _preemptedResourceMB;
  }

  public String getPreemptedResourceVCores() {
    return _preemptedResourceVCores;
  }

  public String getNumNonAMContainerPreempted() {
    return _numNonAMContainerPreempted;
  }

  public String getNumAMContainerPreempted() {
    return _numAMContainerPreempted;
  }
}