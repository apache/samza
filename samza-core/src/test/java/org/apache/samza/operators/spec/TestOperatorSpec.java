package org.apache.samza.operators.spec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import org.apache.samza.operators.IOSystem;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.data.TestOutputMessageEnvelope;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Created by yipan on 8/25/17.
 */
public class TestOperatorSpec implements Serializable {

  @Test
  public void testStreamOperatorSpec() throws IOException, ClassNotFoundException {
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> streamOperatorSpec =
        new StreamOperatorSpec<>(
            m -> new ArrayList<TestOutputMessageEnvelope>() {{ this.add(new TestOutputMessageEnvelope(m.getKey(), m.getMessage().hashCode())); }},
            OperatorSpec.OpCode.MAP, 0);
    StreamOperatorSpec<TestMessageEnvelope, TestOutputMessageEnvelope> cloneOperatorSpec = streamOperatorSpec.copy();
    assertNotEquals(streamOperatorSpec, cloneOperatorSpec);
    assertTrue(streamOperatorSpec.isClone(cloneOperatorSpec));
    assertNotEquals(streamOperatorSpec.getTransformFn(), cloneOperatorSpec.getTransformFn());
  }

  @Test
  public void testInputOperatorSpec() throws IOException, ClassNotFoundException {
    IOSystem ioSystem = () -> "mysystem";
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
    StreamDescriptor.Input<String, Object> inputStream = StreamDescriptor.<String, Object>input("myInput").
        withKeySerde(new StringSerde("UTF-8")).withMsgSerde(objSerde).from(ioSystem);
    InputOperatorSpec<String, Object, TestMessageEnvelope> inputOperatorSpec = new InputOperatorSpec<>(
        inputStream.getStreamSpec(), TestMessageEnvelope::new, 0);
    InputOperatorSpec<String, Object, TestMessageEnvelope> inputOpCopy = inputOperatorSpec.copy();

    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", inputOperatorSpec, inputOpCopy);
    assertTrue(inputOperatorSpec.isClone(inputOpCopy));

  }

  @Test
  public void testOutputOperatorSpec() throws IOException, ClassNotFoundException {
    IOSystem ioSystem = () -> "mysystem";
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
    StreamDescriptor.Output<String, Object> outputStream = StreamDescriptor.<String, Object>output("myOutput").
        withKeySerde(new StringSerde("UTF-8")).withMsgSerde(objSerde).from(ioSystem);
    OutputStreamImpl<String, Object, TestMessageEnvelope> outputStrmImpl = new OutputStreamImpl<>(outputStream, m -> m.getKey(), m -> m.getMessage());
    OutputOperatorSpec<TestMessageEnvelope> outputOperatorSpec = new OutputOperatorSpec<TestMessageEnvelope>(outputStrmImpl,
        OperatorSpec.OpCode.OUTPUT, 0);
    OutputOperatorSpec<TestMessageEnvelope> outputOpCopy = outputOperatorSpec.copy();
    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", outputOperatorSpec, outputOpCopy);
    assertTrue(outputOperatorSpec.isClone(outputOpCopy));
  }

  @Test
  public void testSinkOperatorSpec() throws IOException, ClassNotFoundException {
    SinkFunction<TestMessageEnvelope> sinkFn = (m, c, tc) -> System.out.print(m.toString());
    SinkOperatorSpec<TestMessageEnvelope> sinkOpSpec = new SinkOperatorSpec<>(sinkFn, 0);
    SinkOperatorSpec<TestMessageEnvelope> sinkOpCopy = sinkOpSpec.copy();
    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", sinkOpSpec, sinkOpCopy);
    assertTrue(sinkOpSpec.isClone(sinkOpCopy));
  }

  @Test
  public void testJoinOperatorSpec() throws IOException, ClassNotFoundException {
    OperatorSpec<TestMessageEnvelope, Object> leftOpSpec = new OperatorSpec<TestMessageEnvelope, Object>(
        OperatorSpec.OpCode.INPUT, 0) {
    };
    OperatorSpec<TestMessageEnvelope, Object> rightOpSpec = new OperatorSpec<TestMessageEnvelope, Object>(
        OperatorSpec.OpCode.INPUT, 1) {
    };

    OperatorSpec<TestMessageEnvelope, Object> leftOpCopy = (OperatorSpec<TestMessageEnvelope, Object>) leftOpSpec.copy();

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

    JoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOperatorSpec = new JoinOperatorSpec<>(leftOpSpec, rightOpSpec, joinFn, 50000, 2);
    JoinOperatorSpec<String, Object, Object, TestOutputMessageEnvelope> joinOpCopy = joinOperatorSpec.copy();
    assertNotEquals("Expected deserialized copy of operator spec should not be the same as the original operator spec", joinOperatorSpec, joinOpCopy);
    assertFalse(leftOpCopy.equals(joinOpCopy.getLeftInputOpSpec()));
    assertTrue(leftOpCopy.isClone(joinOpCopy.getLeftInputOpSpec()));
  }
}
