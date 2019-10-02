package org.apache.samza.storage;

import scala.collection.immutable.Map;

import java.io.File;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;

public class TaskStorageManagerFactory {
  public static TaskStorageManager create(TaskName taskName, ContainerStorageManager containerStorageManager,
      Map<String, SystemStream> storeChangelogs, SystemAdmins systemAdmins,
      File loggedStoreBaseDir, Partition changelogPartition,
      Config config, TaskMode taskMode) {
    if (new TaskConfig(config).getTransactionalStateEnabled()) {
      throw new UnsupportedOperationException();
    } else {
      return new NonTransactionalStateTaskStorageManager(taskName, containerStorageManager, storeChangelogs, systemAdmins,
          loggedStoreBaseDir, changelogPartition);
    }
  }
}
