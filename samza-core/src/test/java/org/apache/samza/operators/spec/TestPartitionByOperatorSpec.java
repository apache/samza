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
import java.util.Map;
import org.apache.samza.application.StreamApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for partitionBy operator
 */
public class TestPartitionByOperatorSpec {

  private final Config mockConfig = mock(Config.class);
  private final GenericInputDescriptor testinputDescriptor =
      new GenericSystemDescriptor("mockSystem", "mockFactoryClassName")
          .getInputDescriptor("test-input-1", mock(Serde.class));
  private final String testJobName = "testJob";
  private final String testJobId = "1";
  private final String testRepartitionedStreamName = "parByKey";

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
  }

  @Test
  public void testPartitionBy() {
    MapFunction<Object, String> keyFn = m -> m.toString();
    MapFunction<Object, Object> valueFn = m -> m;
    KVSerde<Object, Object> partitionBySerde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream inputStream = appDesc.getInputStream(testinputDescriptor);
        inputStream.partitionBy(keyFn, valueFn, partitionBySerde, testRepartitionedStreamName);
      }, mockConfig);
    assertEquals(2, streamAppDesc.getInputOperators().size());
    Map<String, InputOperatorSpec> inputOpSpecs = streamAppDesc.getInputOperators();
    assertTrue(inputOpSpecs.keySet().contains(String.format("%s-%s-partition_by-%s", testJobName, testJobId, testRepartitionedStreamName)));
    InputOperatorSpec inputOpSpec = inputOpSpecs.get(String.format("%s-%s-partition_by-%s", testJobName, testJobId, testRepartitionedStreamName));
    assertEquals(String.format("%s-%s-partition_by-%s", testJobName, testJobId, testRepartitionedStreamName), inputOpSpec.getStreamId());
    assertTrue(inputOpSpec.getKeySerde() instanceof NoOpSerde);
    assertTrue(inputOpSpec.getValueSerde() instanceof NoOpSerde);
    assertTrue(inputOpSpec.isKeyed());
    assertNull(inputOpSpec.getTimerFn());
    assertNull(inputOpSpec.getWatermarkFn());
    InputOperatorSpec originInputSpec = inputOpSpecs.get(testinputDescriptor.getStreamId());
    assertTrue(originInputSpec.getRegisteredOperatorSpecs().toArray()[0] instanceof PartitionByOperatorSpec);
    PartitionByOperatorSpec reparOpSpec  = (PartitionByOperatorSpec) originInputSpec.getRegisteredOperatorSpecs().toArray()[0];
    assertEquals(reparOpSpec.getOpId(), String.format("%s-%s-partition_by-%s", testJobName, testJobId, testRepartitionedStreamName));
    assertEquals(reparOpSpec.getKeyFunction(), keyFn);
    assertEquals(reparOpSpec.getValueFunction(), valueFn);
    assertEquals(reparOpSpec.getOutputStream().getStreamId(), reparOpSpec.getOpId());
    assertNull(reparOpSpec.getTimerFn());
    assertNull(reparOpSpec.getWatermarkFn());
  }

  @Test
  public void testPartitionByWithNoSerde() {
    MapFunction<Object, String> keyFn = m -> m.toString();
    MapFunction<Object, Object> valueFn = m -> m;
    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream inputStream = appDesc.getInputStream(testinputDescriptor);
        inputStream.partitionBy(keyFn, valueFn, testRepartitionedStreamName);
      }, mockConfig);
    InputOperatorSpec inputOpSpec = streamAppDesc.getInputOperators().get(
        String.format("%s-%s-partition_by-%s", testJobName, testJobId, testRepartitionedStreamName));
    assertNotNull(inputOpSpec);
    assertNull(inputOpSpec.getKeySerde());
    assertNull(inputOpSpec.getValueSerde());
    assertTrue(inputOpSpec.isKeyed());
    assertNull(inputOpSpec.getTimerFn());
    assertNull(inputOpSpec.getWatermarkFn());
    InputOperatorSpec originInputSpec = streamAppDesc.getInputOperators().get(testinputDescriptor.getStreamId());
    assertTrue(originInputSpec.getRegisteredOperatorSpecs().toArray()[0] instanceof PartitionByOperatorSpec);
    PartitionByOperatorSpec reparOpSpec  = (PartitionByOperatorSpec) originInputSpec.getRegisteredOperatorSpecs().toArray()[0];
    assertEquals(reparOpSpec.getOpId(), String.format("%s-%s-partition_by-%s", testJobName, testJobId, testRepartitionedStreamName));
    assertEquals(reparOpSpec.getKeyFunction(), keyFn);
    assertEquals(reparOpSpec.getValueFunction(), valueFn);
    assertEquals(reparOpSpec.getOutputStream().getStreamId(), reparOpSpec.getOpId());
    assertNull(reparOpSpec.getTimerFn());
    assertNull(reparOpSpec.getWatermarkFn());
  }

  @Test
  public void testCopy() {
    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream inputStream = appDesc.getInputStream(testinputDescriptor);
        inputStream.partitionBy(m -> m.toString(), m -> m, testRepartitionedStreamName);
      }, mockConfig);
    OperatorSpecGraph specGraph = streamAppDesc.getOperatorSpecGraph();
    OperatorSpecGraph clonedGraph = specGraph.clone();
    OperatorSpecTestUtils.assertClonedGraph(specGraph, clonedGraph);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimerFunctionAsKeyFn() {
    TimerMapFn keyFn = new TimerMapFn();
    new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<Object> inputStream = appDesc.getInputStream(testinputDescriptor);
        inputStream.partitionBy(keyFn, m -> m, "parByKey");
      }, mockConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWatermarkFunctionAsKeyFn() {
    WatermarkMapFn keyFn = new WatermarkMapFn();
    new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<Object> inputStream = appDesc.getInputStream(testinputDescriptor);
        inputStream.partitionBy(keyFn, m -> m, "parByKey");
      }, mockConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimerFunctionAsValueFn() {
    TimerMapFn valueFn = new TimerMapFn();
    new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<Object> inputStream = appDesc.getInputStream(testinputDescriptor);
        inputStream.partitionBy(m -> m.toString(), valueFn, "parByKey");
      }, mockConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWatermarkFunctionAsValueFn() {
    WatermarkMapFn valueFn = new WatermarkMapFn();
    new StreamApplicationDescriptorImpl(appDesc -> {
        MessageStream<Object> inputStream = appDesc.getInputStream(testinputDescriptor);
        inputStream.partitionBy(m -> m.toString(), valueFn, "parByKey");
      }, mockConfig);
  }
}
