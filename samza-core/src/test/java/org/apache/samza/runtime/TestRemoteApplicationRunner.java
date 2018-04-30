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

import java.time.Duration;
import org.apache.samza.SamzaException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * A test class for {@link RemoteApplicationRunner}.
 */
public class TestRemoteApplicationRunner {
  @Test
  public void testWaitForFinishReturnsBeforeTimeout() {
    RemoteApplicationRunner runner = spy(new RemoteApplicationRunner(new MapConfig()));
    long startTime = System.currentTimeMillis();
    long timeoutInMs = 5000;
    doReturn(ApplicationStatus.SuccessfulFinish).when(runner).getApplicationStatus(any(JobConfig.class));
    runner.waitForFinish(Duration.ofMillis(timeoutInMs));
    assertTrue(System.currentTimeMillis() - startTime < timeoutInMs);
  }

  @Test
  public void testWaitForFinishTimesout() {
    RemoteApplicationRunner runner = spy(new RemoteApplicationRunner(new MapConfig()));
    long timeoutInMs = 1000;

    try {
      doReturn(ApplicationStatus.Running).when(runner).getApplicationStatus(any(JobConfig.class));
      runner.waitForFinish(Duration.ofMillis(timeoutInMs));
    } catch (SamzaException e) {
      assertNotNull(e);
      assertEquals(e.getCause().getMessage(), "Waiting to shutdown remote application runner timed out.");
    }
  }
}
