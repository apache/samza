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
package org.apache.samza.task;

import org.apache.samza.config.Config;
import org.apache.samza.operators.impl.ChainedOperators;
import org.apache.samza.operators.impl.ChainedOperatorsFactory;
import org.apache.samza.operators.internal.OperatorChainSupplier;
import org.apache.samza.operators.task.StreamOperatorAdaptorTask;
import org.apache.samza.operators.task.StreamOperatorTask;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.Partition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


public class TestStreamOperatorAdaptorTask {
  Field userTaskField = null;
  Field chainedOpsField = null;
  OperatorChainSupplier factory =  new ChainedOperatorsFactory();
  @Before public void prep() throws NoSuchFieldException {
    userTaskField = StreamOperatorAdaptorTask.class.getDeclaredField("userTask");
    chainedOpsField = StreamOperatorAdaptorTask.class.getDeclaredField("operatorChains");
    userTaskField.setAccessible(true);
    chainedOpsField.setAccessible(true);
  }


  @Test public void testConstructor() throws IllegalAccessException {
    StreamOperatorTask userTask = mock(StreamOperatorTask.class);
    StreamOperatorAdaptorTask adaptorTask = new StreamOperatorAdaptorTask(userTask, factory);
    StreamOperatorTask taskMemberVar = (StreamOperatorTask) userTaskField.get(adaptorTask);
    Map<SystemStreamPartition, ChainedOperators> chainsMap = (Map<SystemStreamPartition, ChainedOperators>) chainedOpsField.get(adaptorTask);
    assertEquals(taskMemberVar, userTask);
    assertTrue(chainsMap.isEmpty());
  }

  @Test public void testInit() throws Exception {
    StreamOperatorTask userTask = mock(StreamOperatorTask.class);
    StreamOperatorAdaptorTask adaptorTask = new StreamOperatorAdaptorTask(userTask, factory);
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    Set<SystemStreamPartition> testInputs = new HashSet() {{
      this.add(new SystemStreamPartition("test-sys", "test-strm", new Partition(0)));
      this.add(new SystemStreamPartition("test-sys", "test-strm", new Partition(1)));
    }};
    when(mockContext.getSystemStreamPartitions()).thenReturn(testInputs);
    adaptorTask.init(mockConfig, mockContext);
    verify(userTask, times(1)).initOperators(Mockito.anyCollection());
    Map<SystemStreamPartition, ChainedOperators> chainsMap = (Map<SystemStreamPartition, ChainedOperators>) chainedOpsField.get(adaptorTask);
    assertTrue(chainsMap.size() == 2);
    assertTrue(chainsMap.containsKey(testInputs.toArray()[0]));
    assertTrue(chainsMap.containsKey(testInputs.toArray()[1]));
  }

  // TODO: window and process methods to be added after implementation of ChainedOperators.create()
}
