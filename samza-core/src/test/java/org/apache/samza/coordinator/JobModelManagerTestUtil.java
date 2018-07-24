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

package org.apache.samza.coordinator;

import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.StreamMetadataCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utils to create instances of {@link JobModelManager} in unit tests
 */
public class JobModelManagerTestUtil {

  public static JobModelManager getJobModelManager(Config config, int containerCount, HttpServer server) {
    return getJobModelManagerWithLocalityManager(config, containerCount, null, server);
  }

  public static JobModelManager getJobModelManagerWithLocalityManager(Config config, int containerCount, LocalityManager localityManager, HttpServer server) {
    Map<String, ContainerModel> containers = new java.util.HashMap<>();
    for (int i = 0; i < containerCount; i++) {
      ContainerModel container = new ContainerModel(String.valueOf(i), i, new HashMap<TaskName, TaskModel>());
      containers.put(String.valueOf(i), container);
    }
    JobModel jobModel = new JobModel(config, containers, localityManager);
    return new JobModelManager(jobModel, server, null);
  }

  public static JobModelManager getJobModelManagerUsingReadModel(Config config, int containerCount, StreamMetadataCache streamMetadataCache,
    LocalityManager locManager, HttpServer server) {
    List<String> containerIds = new ArrayList<>();
    for (int i = 0; i < containerCount; i++) {
      containerIds.add(String.valueOf(i));
    }
    JobModel jobModel = JobModelManager.readJobModel(config, new HashMap<>(), locManager, streamMetadataCache, containerIds);
    return new JobModelManager(jobModel, server, null);
  }


}
