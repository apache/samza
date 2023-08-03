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

import com.google.common.collect.ImmutableSet;
import org.apache.samza.config.ApplicationConfig.ApplicationMode;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

public class TestIntermediateStreamManager {

  @Mock
  private Config config;
  @Mock
  private JobGraph jobGraph;

  @Mock
  private StreamEdge intermediateStream;

  private IntermediateStreamManager streamManager;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(config.getInt(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS, StreamEdge.PARTITIONS_UNKNOWN))
        .thenReturn(-1);
    buildInputStreams();
    buildOutputStreams();
    buildIntermediateStream();
    streamManager = new IntermediateStreamManager(config);
  }

  @Test
  public void setIntermediateStreamPartitions() {
    streamManager.setIntermediateStreamPartitions(jobGraph);
    verify(intermediateStream).setPartitionCount(1024);
  }

  @Test
  public void setIntermediateStreamPartitionsForBatchMode() {
    streamManager = spy(new IntermediateStreamManager(config));
    doReturn(ApplicationMode.BATCH).when(streamManager).getAppMode();
    streamManager.setIntermediateStreamPartitions(jobGraph);
    verify(intermediateStream).setPartitionCount(256);
  }

  @Test
  public void setIntermediateStreamPartitionsWithDefaultConfigPropertySet() {
    when(config.getInt(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS, StreamEdge.PARTITIONS_UNKNOWN))
        .thenReturn(128);
    streamManager.setIntermediateStreamPartitions(jobGraph);
    verify(intermediateStream).setPartitionCount(128);

  }

  private void buildInputStreams() {
    StreamEdge input1 = mock(StreamEdge.class);
    when(input1.getPartitionCount()).thenReturn(64);
    when(input1.isIntermediate()).thenReturn(false);
    StreamEdge input2 = mock(StreamEdge.class);
    when(input2.getPartitionCount()).thenReturn(1024);
    when(input2.isIntermediate()).thenReturn(false);

    when(jobGraph.getInputStreams())
        .thenReturn(ImmutableSet.of(input1, input2));
  }

  private void buildIntermediateStream() {
    when(intermediateStream.isIntermediate()).thenReturn(true);
    when(intermediateStream.getPartitionCount()).thenReturn(0);
    when(jobGraph.getIntermediateStreamEdges())
        .thenReturn(ImmutableSet.of(intermediateStream));
  }

  private void buildOutputStreams() {
    StreamEdge output1 = mock(StreamEdge.class);
    when(output1.getPartitionCount()).thenReturn(64);
    when(output1.isIntermediate()).thenReturn(false);
    StreamEdge output2 = mock(StreamEdge.class);
    when(output2.getPartitionCount()).thenReturn(256);
    when(output2.isIntermediate()).thenReturn(false);

    when(jobGraph.getOutputStreams())
        .thenReturn(ImmutableSet.of(output1, output2));
  }
}
