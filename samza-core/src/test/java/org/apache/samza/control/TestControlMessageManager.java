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

package org.apache.samza.control;

import java.util.Collections;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;


public class TestControlMessageManager {

  ControlMessageManager manager;
  SystemStreamPartition ssp;

  @Before
  public void setup() {
    ssp = new SystemStreamPartition("test-system", "test-stream", new Partition(0));
    TaskName taskName = new TaskName("Task 0");
    TaskModel taskModel = new TaskModel(taskName, Collections.singleton(ssp), null);
    ContainerModel containerModel = new ContainerModel("1", 0, Collections.singletonMap(taskName, taskModel));
    manager = new ControlMessageManager("Partition 0", containerModel, Collections.singleton(ssp), null, null);
  }

  @Test
  public void testEndOfStreamMessage() {
    IncomingMessageEnvelope envelope = EndOfStreamManager.buildEndOfStreamEnvelope(ssp);
    // verify the end-of-stream message is handled
    assertNotNull(manager.update(envelope));
  }
}