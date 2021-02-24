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

package org.apache.samza.checkpoint;

import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.storage.StateBackendFactory;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.storage.TaskBackupManager;
import org.apache.samza.storage.TaskRestoreManager;
import org.apache.samza.storage.TaskStorageAdmin;
import org.apache.samza.util.Clock;


public class MockStateCheckpointMarkerFactory implements StateBackendFactory {

  @Override
  public TaskBackupManager getBackupManager(JobModel jobModel, ContainerModel containerModel, TaskModel taskModel,
      Map<String, StorageEngine> taskStores, Config config, Clock clock) {
    return null;
  }

  @Override
  public TaskRestoreManager getRestoreManager(JobModel jobModel, ContainerModel containerModel, TaskModel taskModel,
      Map<String, StorageEngine> taskStores, Config config, Clock clock) {
    return null;
  }

  @Override
  public TaskStorageAdmin getAdmin() {
    return null;
  }

  @Override
  public StateCheckpointPayloadSerde getStateCheckpointPayloadSerde() {
    return new MockStateCheckpointPayloadSerde();
  }
}
