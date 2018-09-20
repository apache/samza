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

package org.apache.samza.coordinator.data;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Random;


/**
 * Table schema for Azure processor table.
 * Denotes a row in the table with PARTITION KEY = Job Model Version and ROW KEY = Processor ID
 * Other fields include integer liveness value to which each processor heartbeats,
 * and boolean isLeader value which denotes whether the processor is a leader or not.
 */
public class ProcessorEntity extends TableServiceEntity {
  private Random rand = new Random();
  private int liveness;
  private boolean isLeader;

  public ProcessorEntity() {}

  public ProcessorEntity(String jobModelVersion, String processorId) {
    this.partitionKey = jobModelVersion;
    this.rowKey = processorId;
    this.isLeader = false;
    this.liveness = rand.nextInt(10000) + 2;
  }

  /**
   * Updates heartbeat by updating the liveness value in the table.
   * Sets the liveness field to a random integer value in order to update the last modified since timestamp of the row in the table.
   * This asserts to the leader that the processor is alive.
   */
  public void updateLiveness() {
    liveness = rand.nextInt(10000) + 2;
  }

  public void setIsLeader(boolean leader) {
    isLeader = leader;
  }

  public boolean getIsLeader() {
    return isLeader;
  }

  public String getJobModelVersion() {
    return partitionKey;
  }

  public String getProcessorId() {
    return rowKey;
  }
}