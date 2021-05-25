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

package org.apache.samza.storage.blobstore.index;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.samza.checkpoint.CheckpointId;


/**
 * Represents metadata associated with a remote snapshot.
 */
public class SnapshotMetadata {
  private final CheckpointId checkpointId;
  private final String jobName;
  private final String jobId;
  private final String taskName;
  private final String storeName;

  public SnapshotMetadata(CheckpointId checkpointId, String jobName, String jobId, String taskName,
      String storeName) {
    Preconditions.checkNotNull(checkpointId);
    Preconditions.checkState(StringUtils.isNotBlank(jobName));
    Preconditions.checkState(StringUtils.isNotBlank(jobId));
    Preconditions.checkState(StringUtils.isNotBlank(taskName));
    Preconditions.checkState(StringUtils.isNotBlank(storeName));
    this.checkpointId = checkpointId;
    this.jobName = jobName;
    this.jobId = jobId;
    this.taskName = taskName;
    this.storeName = storeName;
  }

  public CheckpointId getCheckpointId() {
    return checkpointId;
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

    if (!(o instanceof SnapshotMetadata)) {
      return false;
    }

    SnapshotMetadata that = (SnapshotMetadata) o;

    return new EqualsBuilder()
        .append(getCheckpointId(), that.getCheckpointId())
        .append(getJobName(), that.getJobName())
        .append(getJobId(), that.getJobId())
        .append(getTaskName(), that.getTaskName())
        .append(getStoreName(), that.getStoreName())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getCheckpointId())
        .append(getJobName())
        .append(getJobId())
        .append(getTaskName())
        .append(getStoreName())
        .toHashCode();
  }

  @Override
  public String toString() {
    return "SnapshotMetadata{" +
        "checkpointId=" + checkpointId +
        ", jobName='" + jobName + '\'' +
        ", jobId='" + jobId + '\'' +
        ", taskName='" + taskName + '\'' +
        ", storeName='" + storeName + '\'' +
        '}';
  }
}
