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

package org.apache.samza.runtime;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.job.model.JobModel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest(ContainerLaunchUtil.class)
public class TestContainerLaunchUtil {
  private static final String JOB_NAME = "test-job";
  private static final String JOB_ID = "test-job-i001";
  private static final String CONTAINER_ID = "test-job-container-0001";

  private static final ApplicationDescriptorImpl APP_DESC = mock(ApplicationDescriptorImpl.class);
  private static final JobModel JOB_MODEL = mock(JobModel.class);
  private static final Config CONFIG = mock(Config.class);

  @Test
  public void testRunWithException() throws Exception {
    final CountDownLatch completionLatch = new CountDownLatch(1);
    PowerMockito.mockStatic(ContainerLaunchUtil.class);
    PowerMockito.doReturn(mock(CoordinatorStreamStore.class))
        .when(ContainerLaunchUtil.class, "buildCoordinatorStreamStore", eq(CONFIG), any());
    PowerMockito.doAnswer(invocation -> {
      completionLatch.countDown();
      return null;
    }).when(ContainerLaunchUtil.class, "exitProcess", eq(1));
    PowerMockito.doCallRealMethod()
        .when(ContainerLaunchUtil.class, "run", eq(APP_DESC), eq(JOB_NAME), eq(JOB_ID), eq(CONTAINER_ID), any(), any(),
            eq(JOB_MODEL), eq(CONFIG), any());

    int exitCode = ContainerLaunchUtil.run(APP_DESC, JOB_NAME, JOB_ID, CONTAINER_ID, Optional.empty(), Optional.empty(), JOB_MODEL,
        CONFIG, Optional.empty());
    assertEquals(1, exitCode);
  }

  @Test
  public void testRunSuccessfully() throws Exception {
    int exitCode = 0;
    final CountDownLatch completionLatch = new CountDownLatch(1);
    PowerMockito.mockStatic(ContainerLaunchUtil.class);
    PowerMockito.doReturn(mock(CoordinatorStreamStore.class))
        .when(ContainerLaunchUtil.class, "buildCoordinatorStreamStore", eq(CONFIG), any());
    PowerMockito.doAnswer(invocation -> {
      completionLatch.countDown();
      return null;
    }).when(ContainerLaunchUtil.class, "exitProcess", eq(exitCode));
    PowerMockito.doReturn(exitCode)
        .when(ContainerLaunchUtil.class, "run", eq(APP_DESC), eq(JOB_NAME), eq(JOB_ID), eq(CONTAINER_ID), any(), any(),
            eq(JOB_MODEL), eq(CONFIG), any());
    PowerMockito.doCallRealMethod()
        .when(ContainerLaunchUtil.class, "run", eq(APP_DESC), eq(JOB_NAME), eq(JOB_ID), eq(CONTAINER_ID), any(), any(),
            eq(JOB_MODEL));

    ContainerLaunchUtil.run(APP_DESC, JOB_NAME, JOB_ID, CONTAINER_ID, Optional.empty(), Optional.empty(), JOB_MODEL);
    assertTrue(completionLatch.await(1, TimeUnit.SECONDS));
  }
}
