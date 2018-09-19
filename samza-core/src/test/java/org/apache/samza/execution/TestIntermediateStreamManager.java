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
package org.apache.samza.execution;

import org.apache.samza.application.StreamApplicationDescriptorImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link IntermediateStreamManager}
 */
public class TestIntermediateStreamManager extends ExecutionPlannerTestBase {

  @Test
  public void testCalculateRepartitionJoinTopicPartitions() {
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionJoinStreamApplication(), mockConfig);
    IntermediateStreamManager partitionPlanner = new IntermediateStreamManager(mockConfig, mockStreamAppDesc.getInputOperators().values());
    JobGraph mockGraph = new ExecutionPlanner(mockConfig, mock(StreamManager.class))
        .createJobGraph(mockStreamAppDesc);
    // set the input stream partitions
    mockGraph.getInputStreams().forEach(inEdge -> {
        if (inEdge.getStreamSpec().getId().equals(input1Descriptor.getStreamId())) {
          inEdge.setPartitionCount(6);
        } else if (inEdge.getStreamSpec().getId().equals(input2Descriptor.getStreamId())) {
          inEdge.setPartitionCount(5);
        }
      });
    partitionPlanner.calculatePartitions(mockGraph);
    assertEquals(1, mockGraph.getIntermediateStreamEdges().size());
    assertEquals(5, mockGraph.getIntermediateStreamEdges().stream()
        .filter(inEdge -> inEdge.getStreamSpec().getId().equals(intermediateInputDescriptor.getStreamId()))
        .findFirst().get().getPartitionCount());
  }

  @Test
  public void testCalculateRepartitionIntermediateTopicPartitions() {
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionOnlyStreamApplication(), mockConfig);
    IntermediateStreamManager partitionPlanner = new IntermediateStreamManager(mockConfig, mockStreamAppDesc.getInputOperators().values());
    JobGraph mockGraph = new ExecutionPlanner(mockConfig, mock(StreamManager.class))
        .createJobGraph(mockStreamAppDesc);
    // set the input stream partitions
    mockGraph.getInputStreams().forEach(inEdge -> inEdge.setPartitionCount(7));
    partitionPlanner.calculatePartitions(mockGraph);
    assertEquals(1, mockGraph.getIntermediateStreamEdges().size());
    assertEquals(7, mockGraph.getIntermediateStreamEdges().stream()
        .filter(inEdge -> inEdge.getStreamSpec().getId().equals(intermediateInputDescriptor.getStreamId()))
        .findFirst().get().getPartitionCount());
  }

}
