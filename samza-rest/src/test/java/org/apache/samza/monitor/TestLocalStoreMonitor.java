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
import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.samza.config.MapConfig;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.model.Task;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TestLocalStoreMonitor {

  private static File jobDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "samza-test-job/",
                                        "test-jobName-jobId");

  private File taskStoreDir = new File(new File(jobDir, "test-store"), "test-task");

  private Map<String, String> config = ImmutableMap.of(LocalStoreMonitorConfig.CONFIG_LOCAL_STORE_DIR,
      System.getProperty("java.io.tmpdir") + File.separator + "samza-test-job/");

  private LocalStoreMonitor localStoreMonitor;

  // Create mock for jobs client.
  private JobsClient jobsClientMock = Mockito.mock(JobsClient.class);

  private LocalStoreMonitorMetrics localStoreMonitorMetrics;

  private long taskStoreSize;

  @Before
  public void setUp() throws Exception {
    // Make scaffold directories for testing.
    FileUtils.forceMkdir(taskStoreDir);
    taskStoreSize = taskStoreDir.getTotalSpace();

    // Set default return values for methods.
    Mockito.when(jobsClientMock.getJobStatus(Mockito.any()))
           .thenReturn(JobStatus.STOPPED);
    Task task = new Task("localHost", "test-task", 0,
                         new ArrayList<>(), ImmutableList.of("test-store"));
    Mockito.when(jobsClientMock.getTasks(Mockito.any()))
           .thenReturn(ImmutableList.of(task));

    localStoreMonitorMetrics = new LocalStoreMonitorMetrics("TestMonitorName", new NoOpMetricsRegistry());

    // Initialize the local store monitor with mock and config
    localStoreMonitor = new LocalStoreMonitor(new LocalStoreMonitorConfig(new MapConfig(config)),
                                              localStoreMonitorMetrics,
                                              jobsClientMock);
  }

  @After
  public void cleanUp() throws Exception {
    // clean up the temp files created
    FileUtils.deleteDirectory(taskStoreDir);
  }

  // TODO: Fix in SAMZA-1183
  //@Test
  public void shouldDeleteLocalTaskStoreWhenItHasNoOffsetFile() throws Exception {
    localStoreMonitor.monitor();
    assertTrue("Task store directory should not exist.", !taskStoreDir.exists());
    assertEquals(taskStoreSize, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
    assertEquals(2, localStoreMonitorMetrics.noOfDeletedTaskPartitionStores.getCount());
  }

  @Test
  public void shouldDeleteLocalStoreWhenLastModifiedTimeOfOffsetFileIsGreaterThanOffsetTTL()
      throws Exception {
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
    Mockito.when(jobsClientMock.getJobStatus(Mockito.any()))
           .thenReturn(JobStatus.STARTED);
    File offsetFile = createOffsetFile(taskStoreDir);
    localStoreMonitor.monitor();
    assertTrue("Offset file should exist.", offsetFile.exists());
    assertEquals(0, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
  }

  // TODO: Fix in SAMZA-1183
  //@Test
  public void shouldDeleteTaskStoreWhenTaskPreferredStoreIsNotLocalHost() throws Exception {
    Task task = new Task("notLocalHost", "test-task", 0,
                         new ArrayList<>(), ImmutableList.of("test-store"));
    Mockito.when(jobsClientMock.getTasks(Mockito.any()))
           .thenReturn(ImmutableList.of(task));
    localStoreMonitor.monitor();
    assertTrue("Task store directory should not exist.", !taskStoreDir.exists());
    assertEquals(taskStoreSize, localStoreMonitorMetrics.diskSpaceFreedInBytes.getCount());
    assertEquals(2, localStoreMonitorMetrics.noOfDeletedTaskPartitionStores.getCount());
  }

  private static File createOffsetFile(File taskStoreDir) throws Exception {
    File offsetFile = new File(taskStoreDir, "OFFSET");
    offsetFile.createNewFile();
    return offsetFile;
  }
}
