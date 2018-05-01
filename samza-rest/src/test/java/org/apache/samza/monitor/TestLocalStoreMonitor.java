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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.samza.config.MapConfig;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.*;


public class TestLocalStoreMonitor {

  private static Logger LOG = LoggerFactory.getLogger(TestLocalStoreMonitor.class);

  private File localStoreDir;

  private File jobDir;

  private File taskStoreDir;

  private Map<String, String> config;

  private LocalStoreMonitor localStoreMonitor;

  // Create mock for jobs client.
  private JobsClient jobsClientMock = Mockito.mock(JobsClient.class);

  private LocalStoreMonitorMetrics localStoreMonitorMetrics;

  private long taskStoreSize;

  @Before
  public void setUp() throws Exception {

    // Create a different local store directory every time the test is executed to avoid
    // intermittent test failures due to previous unsuccessful test re-runs.
    localStoreDir = Files.createTempDir();
    jobDir = new File(localStoreDir, "test-jobName-jobId");
    taskStoreDir = new File(new File(jobDir, "test-store"), "test-task");

    config = ImmutableMap.of(LocalStoreMonitorConfig.CONFIG_LOCAL_STORE_DIR, localStoreDir.getCanonicalPath());

    // Make scaffold directories for testing.
    FileUtils.forceMkdir(taskStoreDir);
    taskStoreSize = taskStoreDir.getTotalSpace();

    // Set default return values for methods.
    Mockito.when(jobsClientMock.getJobStatus(Mockito.any())).thenReturn(JobStatus.STOPPED);
    Task task = new Task("localHost", "test-task", "0", new ArrayList<>(), ImmutableList.of("test-store"));
    Mockito.when(jobsClientMock.getTasks(Mockito.any())).thenReturn(ImmutableList.of(task));

    localStoreMonitorMetrics = new LocalStoreMonitorMetrics("TestMonitorName", new NoOpMetricsRegistry());

    // Initialize the local store monitor with mock and config
    localStoreMonitor =
        new LocalStoreMonitor(new LocalStoreMonitorConfig(new MapConfig(config)), localStoreMonitorMetrics,
            jobsClientMock);
  }

  @After
  public void cleanUp() {
    // Clean up the entire temp local store directory and all files underneath it.
    try {
      FileUtils.deleteDirectory(localStoreDir);
    } catch (IOException e) {
      // Happens when task store can't be deleted after test finishes.
      LOG.error("Deletion of directory: {} resulted in the exception: {}.", new Object[]{localStoreDir, e});
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void shouldDeleteLocalTaskStoreWhenItHasNoOffsetFile() throws Exception {
    localStoreMonitor.monitor();
    assertTrue("Task store directory should not exist.", !taskStoreDir.exists());
    assertEquals(taskStoreSize, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
    assertEquals(1, localStoreMonitorMetrics.noOfDeletedTaskPartitionStores.getCount());
  }

  @Test
  public void shouldDeleteLocalStoreWhenLastModifiedTimeOfOffsetFileIsGreaterThanOffsetTTL() throws Exception {
    File offsetFile = createOffsetFile(taskStoreDir);
    offsetFile.setLastModified(0);
    localStoreMonitor.monitor();
    assertTrue("Offset file should not exist.", !offsetFile.exists());
    assertEquals(0, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
  }

  @Test
  public void shouldDeleteInActiveLocalStoresOfTheJob() throws Exception {
    File inActiveStoreDir = new File(jobDir, "inActiveStore");
    FileUtils.forceMkdir(inActiveStoreDir);
    File inActiveTaskDir = new File(inActiveStoreDir, "test-task");
    FileUtils.forceMkdir(inActiveTaskDir);
    long inActiveTaskDirSize = inActiveTaskDir.getTotalSpace();
    localStoreMonitor.monitor();
    assertTrue("Inactive task store directory should not exist.", !inActiveTaskDir.exists());
    assertEquals(taskStoreSize + inActiveTaskDirSize, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
    assertEquals(2, localStoreMonitorMetrics.noOfDeletedTaskPartitionStores.getCount());
    FileUtils.deleteDirectory(inActiveStoreDir);
  }

  @Test
  public void shouldDoNothingWhenLastModifiedTimeOfOffsetFileIsLessThanOffsetTTL() throws Exception {
    File offsetFile = createOffsetFile(taskStoreDir);
    localStoreMonitor.monitor();
    assertTrue("Offset file should exist.", offsetFile.exists());
    assertEquals(0, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
  }

  @Test
  public void shouldDoNothingWhenTheJobIsRunning() throws Exception {
    Mockito.when(jobsClientMock.getJobStatus(Mockito.any())).thenReturn(JobStatus.STARTED);
    File offsetFile = createOffsetFile(taskStoreDir);
    localStoreMonitor.monitor();
    assertTrue("Offset file should exist.", offsetFile.exists());
    assertEquals(0, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
  }

  @Test
  public void shouldDeleteTaskStoreWhenTaskPreferredStoreIsNotLocalHost() throws Exception {
    Task task = new Task("notLocalHost", "test-task", "0", new ArrayList<>(), ImmutableList.of("test-store"));
    Mockito.when(jobsClientMock.getTasks(Mockito.any())).thenReturn(ImmutableList.of(task));
    localStoreMonitor.monitor();
    assertTrue("Task store directory should not exist.", !taskStoreDir.exists());
    assertEquals(taskStoreSize, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
    assertEquals(1, localStoreMonitorMetrics.noOfDeletedTaskPartitionStores.getCount());
  }

  @Test
  public void shouldContinueLocalStoreCleanUpAfterFailureToCleanUpStoreOfAJob() throws Exception {
    File testFailingJobDir = new File(localStoreDir, "test-jobName-jobId-1");

    File testFailingTaskStoreDir = new File(new File(testFailingJobDir, "test-store"), "test-task");

    FileUtils.forceMkdir(testFailingTaskStoreDir);

    // For job: test-jobName-jobId-1, throw up in getTasks call and
    // expect the cleanup to succeed for other job: test-jobName-jobId.
    Mockito.doThrow(new RuntimeException("Dummy exception message."))
        .when(jobsClientMock)
        .getTasks(new JobInstance("test-jobName", "jobId-1"));

    Task task = new Task("notLocalHost", "test-task", "0", new ArrayList<>(), ImmutableList.of("test-store"));

    Mockito.when(jobsClientMock.getTasks(new JobInstance("test-jobName", "jobId"))).thenReturn(ImmutableList.of(task));

    Map<String, String> configMap = new HashMap<>(config);
    configMap.put(LocalStoreMonitorConfig.CONFIG_IGNORE_FAILURES, "true");

    LocalStoreMonitor localStoreMonitor =
        new LocalStoreMonitor(new LocalStoreMonitorConfig(new MapConfig(configMap)), localStoreMonitorMetrics,
            jobsClientMock);

    localStoreMonitor.monitor();

    // Non failing job directory should be cleaned up.
    assertTrue("Task store directory should not exist.", !taskStoreDir.exists());
    FileUtils.deleteDirectory(testFailingJobDir);
  }

  private static File createOffsetFile(File taskStoreDir) throws Exception {
    File offsetFile = new File(taskStoreDir, "OFFSET");
    offsetFile.createNewFile();
    return offsetFile;
  }
}
