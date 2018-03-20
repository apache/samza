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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.data.TestOutputMessageEnvelope;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.table.TableSpec;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for all {@link OperatorSpec}
 */
public class TestOperatorSpec implements Serializable {

  @Test
  public void testStreamOperatorSpec() throws IOException, ClassNotFoundException {
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        new StreamOperatorSpec<>(
            (TestMessageEnvelope m) -> new ArrayList<TestOutputMessageEnvelope>() { { this.add(new TestOutputMessageEnvelope(m.getKey(), m.getMessage().hashCode())); } },
            OperatorSpec.OpCode.MAP, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec = streamOperatorSpec.copy();
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
  }

  @Test
  public void testMapWithLambda() throws IOException, ClassNotFoundException {
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        OperatorSpecs.<TestMessageEnvelope, TestOutputMessageEnvelope>createMapOperatorSpec(m -> new TestOutputMessageEnvelope(m.getKey(), m.getMessage().hashCode()), "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec = streamOperatorSpec.copy();
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
  }

  @Test
  public void testInputOperatorSpec() throws IOException, ClassNotFoundException {
    Serde<Object> objSerde = new Serde<Object>() {

      @Override
      public Object fromBytes(byte[] bytes) {
        return null;
      }

      @Override
      public byte[] toBytes(Object object) {
        return new byte[0];
      }
    };

    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    InputOperatorSpec<String, Object> inputOperatorSpec = new InputOperatorSpec<>(
        mockStreamSpec, new StringSerde("UTF-8"), objSerde, true, "op0");
    InputOperatorSpec<String, Object> inputOpCopy = inputOperatorSpec.copy();

    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", inputOperatorSpec, inputOpCopy);
    assertTrue(inputOperatorSpec.isClone(inputOpCopy));

  }

  @Test
  public void testOutputOperatorSpec() throws IOException, ClassNotFoundException {
    Serde<Object> objSerde = new Serde<Object>() {

      @Override
      public Object fromBytes(byte[] bytes) {
        return null;
      }

      @Override
      public byte[] toBytes(Object object) {
        return new byte[0];
      }
    };
    StreamSpec mockStreamSpec = mock(StreamSpec.class);
    OutputStreamImpl<KV<String, Object>> outputStrmImpl = new OutputStreamImpl<>(mockStreamSpec, new StringSerde("UTF-8"), objSerde, true);
    OutputOperatorSpec<KV<String, Object>> outputOperatorSpec = new OutputOperatorSpec<KV<String, Object>>(outputStrmImpl, "op0");
    OutputOperatorSpec<KV<String, Object>> outputOpCopy = outputOperatorSpec.copy();
    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", outputOperatorSpec, outputOpCopy);
    assertTrue(outputOperatorSpec.isClone(outputOpCopy));
  }

  @Test
  public void testSinkOperatorSpec() throws IOException, ClassNotFoundException {
    SinkFunction<TestMessageEnvelope> sinkFn = (m, c, tc) -> System.out.print(m.toString());
    SinkOperatorSpec<TestMessageEnvelope> sinkOpSpec = new SinkOperatorSpec<>(sinkFn, "op0");
    SinkOperatorSpec<TestMessageEnvelope> sinkOpCopy = sinkOpSpec.copy();
    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", sinkOpSpec, sinkOpCopy);
    assertTrue(sinkOpSpec.isClone(sinkOpCopy));
  }

  @Test
  public void testJoinOperatorSpec() throws IOException, ClassNotFoundException {

    OperatorSpec<TestMessageEnvelope, Object> leftOpSpec = new OperatorSpec<TestMessageEnvelope, Object>(
        OperatorSpec.OpCode.INPUT, "op0") {
      @Override
      public WatermarkFunction getWatermarkFn() {
        return null;
      }
      @Override
      public TimerFunction getTimerFn() {
        return null;
      }
    };
    OperatorSpec<TestMessageEnvelope, Object> rightOpSpec = new OperatorSpec<TestMessageEnvelope, Object>(
        OperatorSpec.OpCode.INPUT, "op1") {
      @Override
      public WatermarkFunction getWatermarkFn() {
        return null;
      }
      @Override
      public TimerFunction getTimerFn() {
        return null;
      }
    };

    Serde<Object> objSerde = new Serde<Object>() {

      @Override
      public Object fromBytes(byte[] bytes) {
        return null;
      }

      @Override
      public byte[] toBytes(Object object) {
        return new byte[0];
      }
    };

    JoinFunction<String, Object, Object, TestOutputMessageEnvelope> joinFn = new JoinFunction<String, Object, Object, TestOutputMessageEnvelope>() {
      @Override
      public TestOutputMessageEnvelope apply(Object message, Object otherMessage) {
        return new TestOutputMessageEnvelope(message.toString(), message.hashCode() + otherMessage.hashCode());
      }

      @Override
      public String getFirstKey(Object message) {
        return message.toString();
      }

      @Override
      public String getSecondKey(Object message) {
        return message.toString();
      }
    };

    JoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOperatorSpec =
        new JoinOperatorSpec<>(leftOpSpec, rightOpSpec, joinFn, new StringSerde("UTF-8"), objSerde, objSerde, 50000, "op2");
    JoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOpCopy = joinOperatorSpec.copy();
    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", joinOperatorSpec, joinOpCopy);
    assertTrue(joinOperatorSpec.isClone(joinOpCopy));
    assertNull(joinOpCopy.getLeftInputOpSpec());
    assertNull(joinOpCopy.getRightInputOpSpec());
  }

  @Test
  public void testStreamTableJoinOperatorSpec() throws IOException, ClassNotFoundException {
    StreamTableJoinFunction<String, Object, Object, TestOutputMessageEnvelope> joinFn = new StreamTableJoinFunction<String, Object, Object, TestOutputMessageEnvelope>() {
      @Override
      public TestOutputMessageEnvelope apply(Object message, Object record) {
        return new TestOutputMessageEnvelope(message.toString(), message.hashCode() + record.hashCode());
      }

      @Override
      public String getMessageKey(Object message) {
        return message.toString();
      }

      @Override
      public String getRecordKey(Object record) {
        return record.toString();
      }
    };

    TableSpec tableSpec = new TableSpec("table-0", KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>()), "my.table.provider.class",
        new MapConfig(new HashMap<String, String>() { { this.put("config1", "value1"); this.put("config2", "value2"); } }));

    StreamTableJoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOperatorSpec =
        new StreamTableJoinOperatorSpec<>(tableSpec, joinFn, "join-3");

    StreamTableJoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOpSpecCopy = joinOperatorSpec.copy();
    assertNotEquals(joinOpSpecCopy, joinOperatorSpec);
    assertEquals(joinOpSpecCopy.getOpId(), joinOperatorSpec.getOpId());
    assertTrue(joinOpSpecCopy.getTableSpec() != joinOperatorSpec.getTableSpec());
    assertEquals(joinOpSpecCopy.getTableSpec().getId(), joinOperatorSpec.getTableSpec().getId());
    assertEquals(joinOpSpecCopy.getTableSpec().getTableProviderFactoryClassName(), joinOperatorSpec.getTableSpec().getTableProviderFactoryClassName());
  }

  @Test
  public void testSendToTableOperatorSpec() throws IOException, ClassNotFoundException {
    TableSpec tableSpec = new TableSpec("table-0", KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>()), "my.table.provider.class",
        new MapConfig(new HashMap<String, String>() { { this.put("config1", "value1"); this.put("config2", "value2"); } }));
    SendToTableOperatorSpec<String, Integer> sendOpSpec =
        new SendToTableOperatorSpec<>(tableSpec, "output-1");
    SendToTableOperatorSpec<String, Integer> sendToCopy = sendOpSpec.copy();
    assertNotEquals(sendToCopy, sendOpSpec);
    assertEquals(sendToCopy.getOpId(), sendOpSpec.getOpId());
    assertTrue(sendToCopy.getTableSpec() != sendOpSpec.getTableSpec() && sendToCopy.getTableSpec().equals(sendOpSpec.getTableSpec()));
  }

  @Test
  public void testBroadcastOperatorSpec() throws IOException, ClassNotFoundException {
    OutputStreamImpl<TestOutputMessageEnvelope> outputStream =
        new OutputStreamImpl<>(new StreamSpec("output-0", "outputStream-0", "kafka"), new StringSerde("UTF-8"), new JsonSerdeV2<TestOutputMessageEnvelope>(), true);
    BroadcastOperatorSpec<TestOutputMessageEnvelope> broadcastOpSpec = new BroadcastOperatorSpec<>(outputStream, "broadcast-1");
    BroadcastOperatorSpec<TestOutputMessageEnvelope> broadcastOpCopy = broadcastOpSpec.copy();
    assertNotEquals(broadcastOpCopy, broadcastOpSpec);
    assertEquals(broadcastOpCopy.getOpId(), broadcastOpSpec.getOpId());
    assertTrue(broadcastOpCopy.getOutputStream() != broadcastOpSpec.getOutputStream());
    assertEquals(broadcastOpCopy.getOutputStream().getSystemStream(), broadcastOpSpec.getOutputStream().getSystemStream());
    assertEquals(broadcastOpCopy.getOutputStream().isKeyed(), broadcastOpSpec.getOutputStream().isKeyed());
  }
}
