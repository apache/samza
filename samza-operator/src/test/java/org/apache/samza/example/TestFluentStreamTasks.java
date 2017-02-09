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
package org.apache.samza.example;

import java.lang.reflect.Field;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.operators.impl.OperatorGraph;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskContext;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link StreamOperatorTask}
 */
public class TestFluentStreamTasks {

  private final Set<SystemStreamPartition> inputPartitions = new HashSet<SystemStreamPartition>() { {
      for (int i = 0; i < 4; i++) {
        this.add(new SystemStreamPartition("my-system", String.format("my-topic%d", i), new Partition(i)));
      }
    } };

  @Test
  public void testUserTask() throws Exception {
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    when(mockContext.getSystemStreamPartitions()).thenReturn(this.inputPartitions);
    WindowGraph userTask = new WindowGraph(this.inputPartitions);
    StreamOperatorTask adaptorTask = new StreamOperatorTask(userTask);
    Field pipelineMapFld = StreamOperatorTask.class.getDeclaredField("operatorGraph");
    pipelineMapFld.setAccessible(true);
    OperatorGraph opGraph = (OperatorGraph) pipelineMapFld.get(adaptorTask);

    adaptorTask.init(mockConfig, mockContext);
    this.inputPartitions.forEach(partition -> {
        assertNotNull(opGraph.get(partition.getSystemStream()));
      });
  }

  @Test
  public void testSplitTask() throws Exception {
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    when(mockContext.getSystemStreamPartitions()).thenReturn(this.inputPartitions);
    BroadcastGraph splitTask = new BroadcastGraph(this.inputPartitions);
    StreamOperatorTask adaptorTask = new StreamOperatorTask(splitTask);
    Field pipelineMapFld = StreamOperatorTask.class.getDeclaredField("operatorGraph");
    pipelineMapFld.setAccessible(true);
    OperatorGraph opGraph = (OperatorGraph) pipelineMapFld.get(adaptorTask);

    adaptorTask.init(mockConfig, mockContext);
    this.inputPartitions.forEach(partition -> {
        assertNotNull(opGraph.get(partition.getSystemStream()));
      });
  }

  @Test
  public void testJoinTask() throws Exception {
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    when(mockContext.getSystemStreamPartitions()).thenReturn(this.inputPartitions);
    JoinGraph joinTask = new JoinGraph(this.inputPartitions);
    StreamOperatorTask adaptorTask = new StreamOperatorTask(joinTask);
    Field pipelineMapFld = StreamOperatorTask.class.getDeclaredField("operatorGraph");
    pipelineMapFld.setAccessible(true);
    OperatorGraph opGraph = (OperatorGraph) pipelineMapFld.get(adaptorTask);

    adaptorTask.init(mockConfig, mockContext);
    this.inputPartitions.forEach(partition -> {
        assertNotNull(opGraph.get(partition.getSystemStream()));
      });
  }

}
