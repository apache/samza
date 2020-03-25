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
package org.apache.samza.storage;

import java.io.File;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;

/**
 * Factory class to create {@link TaskRestoreManager}.
 */
class TaskRestoreManagerFactory {

  public static TaskRestoreManager create(
      TaskModel taskModel,
      Map<String, SystemStream> changelogSystemStreams,
      Map<String, StorageEngine> taskStores,
      SystemAdmins systemAdmins,
      StreamMetadataCache streamMetadataCache,
      SSPMetadataCache sspMetadataCache,
      Map<String, SystemConsumer> storeConsumers,
      int maxChangeLogStreamPartitions,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory,
      Config config,
      Clock clock) {

    if (new TaskConfig(config).getTransactionalStateRestoreEnabled()) {
      // Create checkpoint-snapshot based state restoration which is transactional.
      return new TransactionalStateTaskRestoreManager(
          taskModel,
          taskStores,
          changelogSystemStreams,
          systemAdmins,
          storeConsumers,
          sspMetadataCache,
          loggedStoreBaseDirectory,
          nonLoggedStoreBaseDirectory,
          config,
          clock
      );
    } else {
      // Create legacy offset-file based state restoration which is NOT transactional.
      return new NonTransactionalStateTaskRestoreManager(
          taskModel,
          changelogSystemStreams,
          taskStores,
          systemAdmins,
          streamMetadataCache,
          storeConsumers,
          maxChangeLogStreamPartitions,
          loggedStoreBaseDirectory,
          nonLoggedStoreBaseDirectory,
          config,
          clock);
    }
  }
}
