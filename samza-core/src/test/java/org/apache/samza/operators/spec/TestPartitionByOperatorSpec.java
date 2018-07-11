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
package org.apache.samza.operators.spec;

import java.util.Collection;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.StreamSpec;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for partitionBy operator
 */
public class TestPartitionByOperatorSpec {

  private final ApplicationRunner mockRunner = mock(ApplicationRunner.class);
  private final Config mockConfig = mock(Config.class);
  private final String testInputId = "test-input-1";
  private final String testJobName = "testJob";
  private final String testJobId = "1";
  private final String testReparStreamName = "parByKey";
  private StreamGraphSpec graphSpec = null;

  class TimerMapFn implements MapFunction<Object, String>, TimerFunction<String, Object> {

    @Override
    public String apply(Object message) {
      return message.toString();
    }

    @Override
    public void registerTimer(TimerRegistry<String> timerRegistry) {

    }

    @Override
    public Collection<Object> onTimer(String key, long timestamp) {
      return null;
    }
  }

  class WatermarkMapFn implements MapFunction<Object, String>, WatermarkFunction<Object> {

    @Override
    public String apply(Object message) {
      return message.toString();
    }

    @Override
    public Collection<Object> processWatermark(long watermark) {
      return null;
    }

    @Override
    public Long getOutputWatermark() {
      return null;
    }
  }

  @Before
  public void setup() {
    when(mockConfig.get(JobConfig.JOB_NAME())).thenReturn(testJobName);
    when(mockConfig.get(JobConfig.JOB_ID(), "1")).thenReturn(testJobId);
    StreamSpec inputSpec1 = new StreamSpec(testInputId, testInputId, "kafka");
    when(mockRunner.getStreamSpec(testInputId)).thenReturn(inputSpec1);
    String intermediateStreamName = String.format("%s-%s-partition_by-%s", testJobName, testJobId, testReparStreamName);
    StreamSpec intermediateSpec1 = new StreamSpec(intermediateStreamName, intermediateStreamName, "kafka");
    when(mockRunner.getStreamSpec(intermediateStreamName)).thenReturn(intermediateSpec1);
    graphSpec = new StreamGraphSpec(mockRunner, mockConfig);
  }

  @Test
  public void testPartitionBy() {
    MessageStream inputStream = graphSpec.getInputStream(testInputId);
    MapFunction<Object, String> keyFn = m -> m.toString();
    MapFunction<Object, Object> valueFn = m -> m;
    MessageStream<KV<String, Object>>
        reparStream = inputStream.partitionBy(keyFn, valueFn, testReparStreamName);
    InputOperatorSpec inputOpSpec = (InputOperatorSpec) Whitebox.getInternalState(reparStream, "operatorSpec");
    assertEquals(inputOpSpec.getStreamSpec().getId(), String.format("%s-%s-partition_by-%s", testJobName, testJobId, testReparStreamName));
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertTrue(inputOpSpec.getValueSerde() instanceof NoOpSerde);
    assertTrue(inputOpSpec.isKeyed());
    assertNull(inputOpSpec.getTimerFn());
    assertNull(inputOpSpec.getWatermarkFn());
    InputOperatorSpec originInputSpec = (InputOperatorSpec) Whitebox.getInternalState(inputStream, "operatorSpec");
    assertTrue(originInputSpec.getRegisteredOperatorSpecs().toArray()[0] instanceof PartitionByOperatorSpec);
    PartitionByOperatorSpec reparOpSpec  = (PartitionByOperatorSpec) originInputSpec.getRegisteredOperatorSpecs().toArray()[0];
    assertEquals(reparOpSpec.getOpId(), String.format("%s-%s-partition_by-%s", testJobName, testJobId, testReparStreamName));
    assertEquals(reparOpSpec.getKeyFunction(), keyFn);
    assertEquals(reparOpSpec.getValueFunction(), valueFn);
    assertEquals(reparOpSpec.getOutputStream().getStreamSpec(), new StreamSpec(reparOpSpec.getOpId(), reparOpSpec.getOpId(), "kafka"));
    assertNull(reparOpSpec.getTimerFn());
    assertNull(reparOpSpec.getWatermarkFn());
  }

  @Test
  public void testCopy() {
    MessageStream inputStream = graphSpec.getInputStream(testInputId);
    inputStream.partitionBy(m -> m.toString(), m -> m, testReparStreamName);
    OperatorSpecGraph specGraph = graphSpec.getOperatorSpecGraph();
    OperatorSpecGraph clonedGraph = specGraph.clone();
    OperatorSpecTestUtils.assertClonedGraph(specGraph, clonedGraph);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimerFunctionAsKeyFn() {
    TimerMapFn keyFn = new TimerMapFn();
    MessageStream<Object> inputStream = graphSpec.getInputStream(testInputId);
    inputStream.partitionBy(keyFn, m -> m, "parByKey");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWatermarkFunctionAsKeyFn() {
    WatermarkMapFn keyFn = new WatermarkMapFn();
    MessageStream<Object> inputStream = graphSpec.getInputStream(testInputId);
    inputStream.partitionBy(keyFn, m -> m, "parByKey");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimerFunctionAsValueFn() {
    TimerMapFn valueFn = new TimerMapFn();
    MessageStream<Object> inputStream = graphSpec.getInputStream(testInputId);
    inputStream.partitionBy(m -> m.toString(), valueFn, "parByKey");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWatermarkFunctionAsValueFn() {
    WatermarkMapFn valueFn = new WatermarkMapFn();
    MessageStream<Object> inputStream = graphSpec.getInputStream(testInputId);
    inputStream.partitionBy(m -> m.toString(), valueFn, "parByKey");
  }
}
