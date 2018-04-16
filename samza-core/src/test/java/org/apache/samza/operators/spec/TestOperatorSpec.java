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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.data.TestOutputMessageEnvelope;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
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
import org.mockito.internal.util.reflection.Whitebox;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for all {@link OperatorSpec}
 */
public class TestOperatorSpec {

  private static class MapWithWatermarkFn implements MapFunction<TestMessageEnvelope, TestOutputMessageEnvelope>, WatermarkFunction<TestOutputMessageEnvelope> {

    @Override
    public Collection<TestOutputMessageEnvelope> processWatermark(long watermark) {
      return null;
    }

    @Override
    public Long getOutputWatermark() {
      return null;
    }

    @Override
    public TestOutputMessageEnvelope apply(TestMessageEnvelope m) {
      return new TestOutputMessageEnvelope(m.getKey(), m.getMessage().hashCode());
    }
  }

  private static class MapWithTimerFn implements MapFunction<TestMessageEnvelope, TestOutputMessageEnvelope>, TimerFunction<String, TestOutputMessageEnvelope> {

    @Override
    public TestOutputMessageEnvelope apply(TestMessageEnvelope m) {
      return new TestOutputMessageEnvelope(m.getKey(), m.getMessage().hashCode());
    }

    @Override
    public void registerTimer(TimerRegistry<String> timerRegistry) {

    }

    @Override
    public Collection<TestOutputMessageEnvelope> onTimer(String key, long timestamp) {
      return null;
    }
  }

  private static class MapWithEnum implements MapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> {
    private OperatorSpecTestUtils.TestEnum type;

    MapWithEnum(OperatorSpecTestUtils.TestEnum type) {
      this.type = type;
    }

    OperatorSpecTestUtils.TestEnum getType() {
      return this.type;
    }

    void setType(OperatorSpecTestUtils.TestEnum type) {
      this.type = type;
    }

    @Override
    public TestOutputMessageEnvelope apply(TestMessageEnvelope m) {
      return new TestOutputMessageEnvelope(m.getKey(), m.getMessage().hashCode());
    }
  }

  private static class TestJoinFunction implements JoinFunction<String, Object, Object, TestOutputMessageEnvelope> {
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
  }

  private static class TestStreamTableJoinFunction implements StreamTableJoinFunction<String, Object, Object, TestOutputMessageEnvelope> {
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
  }

  @Test
  public void testStreamOperatorSpecWithFlatMap() throws IOException, ClassNotFoundException {
    FlatMapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> flatMap = m -> {
      List<TestOutputMessageEnvelope> result = new ArrayList<>();
      result.add(new TestOutputMessageEnvelope(m.getKey(), m.getMessage().hashCode()));
      return result;
    };
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        StreamOperatorSpec.createStreamOperatorSpec(flatMap, OperatorSpec.OpCode.MAP, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec =
        (StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(streamOperatorSpec);
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    Serializable userFn = (Serializable) Whitebox.getInternalState(streamOperatorSpec, "userFn");
    assertEquals(userFn, flatMap);
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
    assertTrue(cloneOperatorSpec.getTransformFn() instanceof FlatMapFunction);
    Serializable clonedUserFn = (Serializable) Whitebox.getInternalState(cloneOperatorSpec, "userFn");
    assertTrue(clonedUserFn instanceof FlatMapFunction);
    assertNotEquals(userFn, clonedUserFn);
    assertNull(streamOperatorSpec.getWatermarkFn());
    assertNull(cloneOperatorSpec.getWatermarkFn());
    assertNull(streamOperatorSpec.getTimerFn());
    assertNull(cloneOperatorSpec.getTimerFn());
  }

  @Test
  public void testStreamOperatorSpecWithMap() throws IOException, ClassNotFoundException {
    MapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> mapFn =
        m -> new TestOutputMessageEnvelope(m.getKey(), m.getMessage().hashCode());
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        OperatorSpecs.createMapOperatorSpec(mapFn, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec =
        (StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(streamOperatorSpec);
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    Serializable userFn = (Serializable) Whitebox.getInternalState(streamOperatorSpec, "userFn");
    assertEquals(userFn, mapFn);
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
    Serializable clonedUserFn = (Serializable) Whitebox.getInternalState(cloneOperatorSpec, "userFn");
    assertTrue(cloneOperatorSpec.getTransformFn() instanceof FlatMapFunction);
    assertTrue(clonedUserFn instanceof MapFunction);
    assertNotEquals(userFn, clonedUserFn);
    assertNull(streamOperatorSpec.getWatermarkFn());
    assertNull(cloneOperatorSpec.getWatermarkFn());
    assertNull(streamOperatorSpec.getTimerFn());
    assertNull(cloneOperatorSpec.getTimerFn());
  }

  @Test
  public void testStreamOperatorSpecWithFilter() throws IOException, ClassNotFoundException {
    FilterFunction<TestMessageEnvelope> filterFn = m -> m.getKey().equals("key1");
    StreamOperatorSpec<TestMessageEnvelope, TestMessageEnvelope> streamOperatorSpec =
        OperatorSpecs.createFilterOperatorSpec(filterFn, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec =
        (StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(streamOperatorSpec);
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    Serializable userFn = (Serializable) Whitebox.getInternalState(streamOperatorSpec, "userFn");
    assertEquals(userFn, filterFn);
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
    Serializable clonedUserFn = (Serializable) Whitebox.getInternalState(cloneOperatorSpec, "userFn");
    assertTrue(cloneOperatorSpec.getTransformFn() instanceof FlatMapFunction);
    assertTrue(clonedUserFn instanceof FilterFunction);
    assertNotEquals(userFn, clonedUserFn);
    assertNull(streamOperatorSpec.getWatermarkFn());
    assertNull(cloneOperatorSpec.getWatermarkFn());
    assertNull(streamOperatorSpec.getTimerFn());
    assertNull(cloneOperatorSpec.getTimerFn());
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
    InputOperatorSpec<String, Object> inputOpCopy = (InputOperatorSpec<String, Object>) OperatorSpecTestUtils.copyOpSpec(inputOperatorSpec);

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
    OutputOperatorSpec<KV<String, Object>> outputOpCopy = (OutputOperatorSpec<KV<String, Object>>) OperatorSpecTestUtils
        .copyOpSpec(outputOperatorSpec);
    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", outputOperatorSpec, outputOpCopy);
    assertTrue(outputOperatorSpec.isClone(outputOpCopy));
  }

  @Test
  public void testSinkOperatorSpec() throws IOException, ClassNotFoundException {
    SinkFunction<TestMessageEnvelope> sinkFn = (m, c, tc) -> System.out.print(m.toString());
    SinkOperatorSpec<TestMessageEnvelope> sinkOpSpec = new SinkOperatorSpec<>(sinkFn, "op0");
    SinkOperatorSpec<TestMessageEnvelope> sinkOpCopy = (SinkOperatorSpec<TestMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(sinkOpSpec);
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

    JoinFunction<String, Object, Object, TestOutputMessageEnvelope> joinFn = new TestJoinFunction();
    JoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOperatorSpec =
        new JoinOperatorSpec<>(leftOpSpec, rightOpSpec, joinFn, new StringSerde("UTF-8"), objSerde, objSerde, 50000, "op2");
    JoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOpCopy =
        (JoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(joinOperatorSpec);
    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", joinOperatorSpec, joinOpCopy);
    assertTrue(joinOperatorSpec.isClone(joinOpCopy));
    assertNull(joinOpCopy.getLeftInputOpSpec());
    assertNull(joinOpCopy.getRightInputOpSpec());
  }

  @Test
  public void testStreamTableJoinOperatorSpec() throws IOException, ClassNotFoundException {
    StreamTableJoinFunction<String, Object, Object, TestOutputMessageEnvelope> joinFn = new TestStreamTableJoinFunction();

    TableSpec tableSpec = new TableSpec("table-0", KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>()), "my.table.provider.class",
        new MapConfig(new HashMap<String, String>() { { this.put("config1", "value1"); this.put("config2", "value2"); } }));

    StreamTableJoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOperatorSpec =
        new StreamTableJoinOperatorSpec<>(tableSpec, joinFn, "join-3");

    StreamTableJoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOpSpecCopy =
        (StreamTableJoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(joinOperatorSpec);
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
    SendToTableOperatorSpec<String, Integer> sendToCopy = (SendToTableOperatorSpec<String, Integer>) OperatorSpecTestUtils
        .copyOpSpec(sendOpSpec);
    assertNotEquals(sendToCopy, sendOpSpec);
    assertEquals(sendToCopy.getOpId(), sendOpSpec.getOpId());
    assertTrue(sendToCopy.getTableSpec() != sendOpSpec.getTableSpec() && sendToCopy.getTableSpec().equals(sendOpSpec.getTableSpec()));
  }

  @Test
  public void testBroadcastOperatorSpec() throws IOException, ClassNotFoundException {
    OutputStreamImpl<TestOutputMessageEnvelope> outputStream =
        new OutputStreamImpl<>(new StreamSpec("output-0", "outputStream-0", "kafka"), new StringSerde("UTF-8"), new JsonSerdeV2<TestOutputMessageEnvelope>(), true);
    BroadcastOperatorSpec<TestOutputMessageEnvelope> broadcastOpSpec = new BroadcastOperatorSpec<>(outputStream, "broadcast-1");
    BroadcastOperatorSpec<TestOutputMessageEnvelope> broadcastOpCopy = (BroadcastOperatorSpec<TestOutputMessageEnvelope>) OperatorSpecTestUtils
        .copyOpSpec(broadcastOpSpec);
    assertNotEquals(broadcastOpCopy, broadcastOpSpec);
    assertEquals(broadcastOpCopy.getOpId(), broadcastOpSpec.getOpId());
    assertTrue(broadcastOpCopy.getOutputStream() != broadcastOpSpec.getOutputStream());
    assertEquals(broadcastOpCopy.getOutputStream().getSystemStream(), broadcastOpSpec.getOutputStream().getSystemStream());
    assertEquals(broadcastOpCopy.getOutputStream().isKeyed(), broadcastOpSpec.getOutputStream().isKeyed());
  }

  @Test
  public void testMapStreamOperatorSpecWithWatermark() throws IOException, ClassNotFoundException {
    MapWithWatermarkFn testMapFn = new MapWithWatermarkFn();

    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        StreamOperatorSpec.createStreamOperatorSpec(
            testMapFn,
            OperatorSpec.OpCode.MAP, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec =
        (StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(streamOperatorSpec);
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
    assertEquals(streamOperatorSpec.getWatermarkFn(), testMapFn);
    assertNotNull(cloneOperatorSpec.getWatermarkFn());
    assertNotEquals(cloneOperatorSpec.getTransformFn(), cloneOperatorSpec.getWatermarkFn());
    assertNull(streamOperatorSpec.getTimerFn());
    assertNull(cloneOperatorSpec.getTimerFn());
  }

  @Test
  public void testMapStreamOperatorSpecWithTimer() throws IOException, ClassNotFoundException {
    MapWithTimerFn testMapFn = new MapWithTimerFn();

    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        StreamOperatorSpec.createStreamOperatorSpec(
            testMapFn,
            OperatorSpec.OpCode.MAP, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec =
        (StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(streamOperatorSpec);
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
    assertNull(streamOperatorSpec.getWatermarkFn());
    assertNull(cloneOperatorSpec.getWatermarkFn());
    assertNotEquals(cloneOperatorSpec.getTransformFn(), cloneOperatorSpec.getWatermarkFn());
    assertEquals(streamOperatorSpec.getTimerFn(), testMapFn);
    assertNotNull(cloneOperatorSpec.getTimerFn());
    assertNotEquals(streamOperatorSpec.getTimerFn(), cloneOperatorSpec.getTimerFn());
  }

  @Test
  public void testStreamOperatorSpecWithMapAndListInClosure() throws IOException, ClassNotFoundException {
    List<Integer> integers = new ArrayList<>(1);
    integers.add(0, 100);
    List<String> keys = new ArrayList<>(1);
    keys.add(0, "test-1");
    MapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> mapFn =
        m -> new TestOutputMessageEnvelope(keys.get(m.getKey().hashCode() % 1), integers.get(m.getMessage().hashCode() % 1));
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        OperatorSpecs.createMapOperatorSpec(mapFn, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec =
        (StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(streamOperatorSpec);
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    Serializable userFn = (Serializable) Whitebox.getInternalState(streamOperatorSpec, "userFn");
    assertEquals(userFn, mapFn);
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
    Serializable clonedUserFn = (Serializable) Whitebox.getInternalState(cloneOperatorSpec, "userFn");
    assertTrue(cloneOperatorSpec.getTransformFn() instanceof FlatMapFunction);
    assertTrue(clonedUserFn instanceof MapFunction);
    assertNotEquals(userFn, clonedUserFn);

    // verify changing the values in the original keys and integers list will change the result of the original map function
    TestMessageEnvelope mockImsg = new TestMessageEnvelope("input-key-x", new String("value-x"));
    assertEquals(((MapFunction) userFn).apply(mockImsg), new TestOutputMessageEnvelope("test-1", 100));
    integers.set(0, 200);
    keys.set(0, "test-2");
    assertEquals(((MapFunction) userFn).apply(mockImsg), new TestOutputMessageEnvelope("test-2", 200));
    // verify that the cloned map function uses a different copy of lists and still yields the same result
    assertEquals(((MapFunction) clonedUserFn).apply(mockImsg), new TestOutputMessageEnvelope("test-1", 100));
  }

  @Test
  public void testStreamOperatorSpecWithMapWithFunctionReference() throws IOException, ClassNotFoundException {
    MapFunction<KV<String, Object>, Object> mapFn = KV::getValue;
    StreamOperatorSpec<KV<String, Object>, Object> streamOperatorSpec =
        OperatorSpecs.createMapOperatorSpec(mapFn, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec =
        (StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(streamOperatorSpec);
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    Serializable userFn = (Serializable) Whitebox.getInternalState(streamOperatorSpec, "userFn");
    assertEquals(userFn, mapFn);
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
    Serializable clonedUserFn = (Serializable) Whitebox.getInternalState(cloneOperatorSpec, "userFn");
    assertTrue(cloneOperatorSpec.getTransformFn() instanceof FlatMapFunction);
    assertTrue(clonedUserFn instanceof MapFunction);
    assertNotEquals(userFn, clonedUserFn);
  }

  @Test
  public void testStreamOperatorSpecWithMapWithEnum() throws IOException, ClassNotFoundException {
    MapFunction<TestMessageEnvelope, TestOutputMessageEnvelope> mapFn = new MapWithEnum(OperatorSpecTestUtils.TestEnum.One);
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        OperatorSpecs.createMapOperatorSpec(mapFn, "op0");
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec =
        (StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope>) OperatorSpecTestUtils.copyOpSpec(streamOperatorSpec);
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    Serializable userFn = (Serializable) Whitebox.getInternalState(streamOperatorSpec, "userFn");
    assertEquals(userFn, mapFn);
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
    Serializable clonedUserFn = (Serializable) Whitebox.getInternalState(cloneOperatorSpec, "userFn");
    assertTrue(cloneOperatorSpec.getTransformFn() instanceof FlatMapFunction);
    assertTrue(clonedUserFn instanceof MapWithEnum);
    assertNotEquals(userFn, clonedUserFn);
    // originally the types should be the same
    assertTrue(((MapWithEnum) userFn).getType() == ((MapWithEnum) clonedUserFn).getType());
    // after changing the type of the cloned user function, the types are different now
    ((MapWithEnum) clonedUserFn).setType(OperatorSpecTestUtils.TestEnum.Two);
    assertTrue(((MapWithEnum) userFn).getType() != ((MapWithEnum) clonedUserFn).getType());
  }
}
