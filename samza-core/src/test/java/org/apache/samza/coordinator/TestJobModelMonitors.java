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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;


public class TestJobModelMonitors {
  @Mock
  private StreamPartitionCountMonitor streamPartitionCountMonitor;
  @Mock
  private StreamRegexMonitor streamRegexMonitor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testMonitorsExist() {
    JobModelMonitors jobModelMonitors = new JobModelMonitors(this.streamPartitionCountMonitor, this.streamRegexMonitor);

    jobModelMonitors.start();
    verify(this.streamPartitionCountMonitor).start();
    verify(this.streamRegexMonitor).start();

    jobModelMonitors.stop();
    verify(this.streamPartitionCountMonitor).stop();
    verify(this.streamRegexMonitor).stop();
  }

  @Test
  public void testMissingMonitors() {
    JobModelMonitors jobModelMonitors = new JobModelMonitors(null, null);
    // expect no failures
    jobModelMonitors.start();
    jobModelMonitors.stop();
  }

  @Test
  public void testStopBeforeStart() {
    JobModelMonitors jobModelMonitors = new JobModelMonitors(this.streamPartitionCountMonitor, this.streamRegexMonitor);
    jobModelMonitors.stop();
    verifyZeroInteractions(this.streamPartitionCountMonitor, this.streamRegexMonitor);
  }
}