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
  private long finishTime;

  @JsonProperty("amContainerLogs")
  private String amContainerLogs;

  @JsonProperty("trackingUI")
  private String trackingHistory;

  @JsonProperty("state")
  private String state;

  @JsonProperty("user")
  private String user;

  @JsonProperty("id")
  private String id;

  @JsonProperty("clusterId")
  private String clusterId;

  @JsonProperty("finalStatus")
  private String finalStatus;

  @JsonProperty("amHostHttpAddress")
  private String amHostHttpAddress;

  @JsonProperty("applicationType")
  private String applicationType;

  @JsonProperty("applicationTags")
  private String applicationTags;

  @JsonProperty("finishedTime")
  private String finishedTime;

  @JsonProperty("preemptedResourceMB")
  private String preemptedResourceMB;

  @JsonProperty("preemptedResourceVCores")
  private String preemptedResourceVCores;

  @JsonProperty("numNonAMContainerPreempted")
  private String numNonAMContainerPreempted;

  @JsonProperty("numAMContainerPreempted")
  private String numAMContainerPreempted;

  @JsonProperty("progress")
  private int progress;

  @JsonProperty("name")
  private String name;

  @JsonProperty("startedTime")
  private long startedTime;

  @JsonProperty("elapsedTime")
  private long elapsedTime;

  @JsonProperty("diagnostics")
  private String diagnostics;

  @JsonProperty("trackingUrl")
  private String trackingUrl;

  @JsonProperty("queue")
  private String queue;

  @JsonProperty("allocatedMB")
  private int allocatedMB;

  @JsonProperty("allocatedVCores")
  private int allocatedVCores;

  @JsonProperty("runningContainers")
  private int runningContainers;

  @JsonProperty("memorySeconds")
  private int memorySeconds;

  @JsonProperty("vcoreSeconds")
  private int vcoreSeconds;

  ApplicationInfo() {

  }

  public ApplicationInfo(long finishTime, String amContainerLogs, String trackingHistory, String state, String user,
      String id, String clusterId, String finalStatus, String amHostHttpAddress, String applicationType,
      String applicationTags, String finishedTime, String preemptedResourceMB, String preemptedResourceVCores,
      String numNonAMContainerPreempted, String numAMContainerPreempted, int progress, String name, long startedTime,
      long elapsedTime, String diagnostics, String trackingUrl, String queue, int allocatedMB, int allocatedVCores,
      int runningContainers, int memorySeconds, int vcoreSeconds) {
    this.finishTime = finishTime;
    this.amContainerLogs = amContainerLogs;
    this.trackingHistory = trackingHistory;
    this.state = state;
    this.user = user;
    this.id = id;
    this.clusterId = clusterId;
    this.finalStatus = finalStatus;
    this.amHostHttpAddress = amHostHttpAddress;
    this.applicationType = applicationType;
    this.applicationTags = applicationTags;
    this.finishedTime = finishedTime;
    this.preemptedResourceMB = preemptedResourceMB;
    this.preemptedResourceVCores = preemptedResourceVCores;
    this.numNonAMContainerPreempted = numNonAMContainerPreempted;
    this.numAMContainerPreempted = numAMContainerPreempted;
    this.progress = progress;
    this.name = name;
    this.startedTime = startedTime;
    this.elapsedTime = elapsedTime;
    this.diagnostics = diagnostics;
    this.trackingUrl = trackingUrl;
    this.queue = queue;
    this.allocatedMB = allocatedMB;
    this.allocatedVCores = allocatedVCores;
    this.runningContainers = runningContainers;
    this.memorySeconds = memorySeconds;
    this.vcoreSeconds = vcoreSeconds;
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
    return finishTime == that.finishTime && progress == that.progress && startedTime == that.startedTime
        && elapsedTime == that.elapsedTime && allocatedMB == that.allocatedMB
        && allocatedVCores == that.allocatedVCores && runningContainers == that.runningContainers
        && memorySeconds == that.memorySeconds && vcoreSeconds == that.vcoreSeconds && Objects.equals(
        amContainerLogs, that.amContainerLogs) && Objects.equals(trackingHistory, that.trackingHistory)
        && Objects.equals(state, that.state) && Objects.equals(user, that.user) && Objects.equals(id, that.id)
        && Objects.equals(clusterId, that.clusterId) && Objects.equals(finalStatus, that.finalStatus)
        && Objects.equals(amHostHttpAddress, that.amHostHttpAddress) && Objects.equals(applicationType,
        that.applicationType) && Objects.equals(applicationTags, that.applicationTags) && Objects.equals(finishedTime, that.finishedTime) && Objects.equals(
        preemptedResourceMB, that.preemptedResourceMB) && Objects
        .equals(preemptedResourceVCores, that.preemptedResourceVCores) && Objects.equals(numNonAMContainerPreempted,
        that.numNonAMContainerPreempted) && Objects.equals(numAMContainerPreempted, that.numAMContainerPreempted)
        && Objects.equals(name, that.name) && Objects.equals(diagnostics, that.diagnostics) && Objects.equals(
        trackingUrl, that.trackingUrl) && Objects.equals(queue, that.queue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(finishTime, amContainerLogs, trackingHistory, state, user, id, clusterId, finalStatus,
        amHostHttpAddress, applicationType, applicationTags, finishedTime, preemptedResourceMB, preemptedResourceVCores,
        numNonAMContainerPreempted, numAMContainerPreempted, progress, name, startedTime, elapsedTime, diagnostics,
        trackingUrl, queue, allocatedMB, allocatedVCores, runningContainers, memorySeconds, vcoreSeconds);
  }

  public long getFinishTime() {
    return finishTime;
  }

  public String getAmContainerLogs() {
    return amContainerLogs;
  }

  public String getTrackingHistory() {
    return trackingHistory;
  }

  public String getState() {
    return state;
  }

  public String getUser() {
    return user;
  }

  public String getId() {
    return id;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getFinalStatus() {
    return finalStatus;
  }

  public String getAmHostHttpAddress() {
    return amHostHttpAddress;
  }

  public int getProgress() {
    return progress;
  }

  public String getName() {
    return name;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public String getQueue() {
    return queue;
  }

  public int getAllocatedMB() {
    return allocatedMB;
  }

  public int getAllocatedVCores() {
    return allocatedVCores;
  }

  public int getRunningContainers() {
    return runningContainers;
  }

  public int getMemorySeconds() {
    return memorySeconds;
  }

  public int getVcoreSeconds() {
    return vcoreSeconds;
  }

  public String getApplicationType() {
    return applicationType;
  }

  public String getApplicationTags() {
    return applicationTags;
  }

  public String getFinishedTime() {
    return finishedTime;
  }

  public String getPreemptedResourceMB() {
    return preemptedResourceMB;
  }

  public String getPreemptedResourceVCores() {
    return preemptedResourceVCores;
  }

  public String getNumNonAMContainerPreempted() {
    return numNonAMContainerPreempted;
  }

  public String getNumAMContainerPreempted() {
    return numAMContainerPreempted;
  }
}