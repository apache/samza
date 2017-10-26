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

package org.apache.samza.checkpoint.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class TaskCheckpointEntity extends TableServiceEntity {

  private String streamName;
  private int partitionId;
  private String offset;

  public TaskCheckpointEntity() {}

  public TaskCheckpointEntity(String taskName, String systemName) {
    this.partitionKey = taskName;
    this.rowKey = systemName;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public String getOffset() {
    return this.offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }
}
