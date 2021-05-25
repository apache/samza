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

package org.apache.samza.storage.blobstore;

import java.util.Optional;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * Metadata associated with every BlobStore request. This class is used to trace a request and to determine the
 * bucket/container informationof the blob.
 */
public class Metadata {
  public static final String SNAPSHOT_INDEX_PAYLOAD_PATH = "snapshot-index";

  private final String payloadPath;
  private final long payloadSize;
  private final String jobName;
  private final String jobId;
  private final String taskName;
  private final String storeName;

  public Metadata(String payloadPath, Optional<Long> payloadSize,
      String jobName, String jobId, String taskName, String storeName) {
    this.payloadPath = payloadPath;
    // Payload size may not be known in advance for requests like getSnapshotIndex, where only blob ID is known. Set -1.
    this.payloadSize = payloadSize.orElse(-1L);
    this.jobName = jobName;
    this.jobId = jobId;
    this.taskName = taskName;
    this.storeName = storeName;
  }

  public String getPayloadPath() {
    return payloadPath;
  }

  public long getPayloadSize() {
    return payloadSize;
  }

  public String getJobName() {
    return jobName;
  }

  public String getJobId() {
    return jobId;
  }

  public String getTaskName() {
    return taskName;
  }

  public String getStoreName() {
    return storeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Metadata)) {
      return false;
    }

    Metadata that = (Metadata) o;

    return new EqualsBuilder().append(getPayloadPath(), that.getPayloadPath())
        .append(getPayloadSize(), that.getPayloadSize())
        .append(getJobName(), that.getJobName())
        .append(getJobId(), that.getJobId())
        .append(getTaskName(), that.getTaskName())
        .append(getStoreName(), that.getStoreName())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(getPayloadPath())
        .append(getPayloadSize())
        .append(getJobName())
        .append(getJobId())
        .append(getTaskName())
        .append(getStoreName())
        .toHashCode();
  }

  @Override
  public String toString() {
    return "Metadata{" + "payloadPath='" + payloadPath + '\'' + ", payloadSize='" + payloadSize + '\''
        + ", jobName='" + jobName + '\'' + ", jobId='" + jobId + '\'' + ", taskName='" + taskName + '\''
        + ", storeName='" + storeName + '\'' + '}';
  }
}
