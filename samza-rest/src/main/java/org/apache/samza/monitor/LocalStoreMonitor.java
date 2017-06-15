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
package org.apache.samza.monitor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.FileUtils;
import org.apache.samza.container.TaskName;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.storage.TaskStorageManager;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This monitor class periodically checks for the presence
 * of stale store directories and deletes them if the
 * samza task that created it is not running
 * for more than X days.
 */
public class LocalStoreMonitor implements Monitor {
  private static final Clock CLOCK = SystemClock.instance();

  private static final Logger LOG = LoggerFactory.getLogger(LocalStoreMonitor.class);

  private static final String OFFSET_FILE_NAME = "OFFSET";

  private final JobsClient jobsClient;

  private final LocalStoreMonitorConfig config;

  private final LocalStoreMonitorMetrics localStoreMonitorMetrics;

  public LocalStoreMonitor(LocalStoreMonitorConfig config,
                           LocalStoreMonitorMetrics localStoreMonitorMetrics,
                           JobsClient jobsClient) {
    Preconditions.checkState(!Strings.isNullOrEmpty(config.getLocalStoreBaseDir()),
                             String.format("%s is not set in config.", LocalStoreMonitorConfig.CONFIG_LOCAL_STORE_DIR));
    this.config = config;
    this.jobsClient = jobsClient;
    this.localStoreMonitorMetrics = localStoreMonitorMetrics;
  }

  /**
   * This monitor method is invoked periodically to delete the stale state stores
   * of dead jobs/tasks.
   * @throws Exception if there was any problem in running the monitor.
   */
  @Override
  public void monitor() throws Exception {
    File localStoreDir = new File(config.getLocalStoreBaseDir());
    Preconditions.checkState(localStoreDir.isDirectory(),
                             String.format("LocalStoreDir: %s is not a directory", localStoreDir.getAbsolutePath()));
    String localHostName = InetAddress.getLocalHost().getHostName();
    for (JobInstance jobInstance : getHostAffinityEnabledJobs(localStoreDir)) {
      File jobDir = new File(localStoreDir,
                             String.format("%s-%s", jobInstance.getJobName(), jobInstance.getJobId()));
      try {
        JobStatus jobStatus = jobsClient.getJobStatus(jobInstance);
        LOG.info("Job: {} has the status: {}.", jobInstance, jobStatus);
        for (Task task : jobsClient.getTasks(jobInstance)) {
          LOG.info("  Evaluating stores for job: ({}) task: ({})", jobInstance, task);
          for (String storeName : jobDir.list(DirectoryFileFilter.DIRECTORY)) {
            /**
             *  A task store is active if all of the following conditions are true:
             *  a) If the store is amongst the active stores of the task.
             *  b) If the job has been started.
             *  c) If the preferred host of the task is the localhost on which the monitor is run.
             */
            if (jobStatus.hasBeenStarted()
                && task.getStoreNames().contains(storeName)
                && task.getPreferredHost().equals(localHostName)) {
              LOG.info(String.format("  Local store %s is actively used by the task: %s.", storeName, task.getTaskName()));
            } else {
              LOG.info(String.format("  Local store %s not used by the task: %s.", storeName, task.getTaskName()));
              markSweepTaskStore(TaskStorageManager.getStorePartitionDir(jobDir, storeName, new TaskName(task.getTaskName())));
            }
          }
        }
      } catch (Exception ex) {
        if (!config.getIgnoreFailures()) {
          throw ex;
        }
        LOG.warn("Config: {} turned on, failures will be ignored. Local store cleanup for job: {} resulted in exception: {}.",
                 new Object[]{LocalStoreMonitorConfig.CONFIG_IGNORE_FAILURES, jobInstance, ex});
      }
    }
  }

  /**
   * Helper method to find and return the list of host affinity enabled jobs on this NM.
   * @param localStoreDirFile the location in which all stores of host affinity enabled jobs are persisted.
   * @return the list of the host affinity enabled jobs that are installed on this NM.
   */
  private static List<JobInstance> getHostAffinityEnabledJobs(File localStoreDirFile) {
    List<JobInstance> jobInstances = new ArrayList<>();
    for (File jobStore : localStoreDirFile.listFiles(File::isDirectory)) {
    // Name of the jobStore(jobStorePath) is of the form : ${job.name}-${job.id}.
      String jobStorePath = jobStore.getName();
      int indexSeparator = jobStorePath.lastIndexOf("-");
      if (indexSeparator != -1) {
        jobInstances.add(new JobInstance(jobStorePath.substring(0, indexSeparator),
                                         jobStorePath.substring(indexSeparator + 1)));
      }
    }
    return jobInstances;
  }

  /**
   * Role of this method is to garbage collect(mark-sweep) the task store.
   * @param taskStoreDir store directory of the task to perform garbage collection.
   *
   * This method cleans up each of the task store directory in two phases.
   *
   *  Phase 1:
   *  Delete the offset file in the task store if (curTime - lastModifiedTimeOfOffsetFile) > offsetTTL.
   *
   *  Phase 2:
   *  Delete the task store directory if the offsetFile does not exist in task store directory.
   *
   * The separate phases are a safety precaution to prevent deleting a store that is currently being used.
   * A running task will recreate the deleted offset file on the next commit. If a task is not running or
   * running on a different host and gets moved to this host, it will not use a store without the offset file.
   *
   * Time interval between the two phases is controlled by this monitor scheduling
   * interval in milli seconds.
   *
   * @throws IOException if there is an exception during the clean up of the task store files.
   */
  private void markSweepTaskStore(File taskStoreDir) throws IOException {
    String taskStorePath = taskStoreDir.getAbsolutePath();
    File offsetFile = new File(taskStoreDir, OFFSET_FILE_NAME);
    if (!offsetFile.exists()) {
      LOG.info("Deleting the task store: {}, since it has no offset file.", taskStorePath);
      long taskStoreSizeInBytes = taskStoreDir.getTotalSpace();
      FileUtils.deleteDirectory(taskStoreDir);
      localStoreMonitorMetrics.diskSpaceFreedInBytes.inc(taskStoreSizeInBytes);
      localStoreMonitorMetrics.noOfDeletedTaskPartitionStores.inc();
    } else if ((CLOCK.currentTimeMillis() - offsetFile.lastModified()) >= config.getOffsetFileTTL()) {
      LOG.info("Deleting the offset file from the store: {}, since the last modified timestamp: {} "
                   + "is older than the configured ttl: {}.",
                  taskStorePath, offsetFile.lastModified(), config.getOffsetFileTTL());
      offsetFile.delete();
    }
  }
}
