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
package org.apache.samza.operators;

import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJoinOperator {
  private final MessageCollector messageCollector = mock(MessageCollector.class);
  private final TaskCoordinator taskCoordinator = mock(TaskCoordinator.class);
  private final Set<Integer> numbers = ImmutableSet.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

  private StreamOperatorTask sot;
  private List<Integer> output = new ArrayList<>();

  @Before
  public void setup() throws Exception {
    output.clear();

    TaskContext taskContext = mock(TaskContext.class);
    when(taskContext.getSystemStreamPartitions()).thenReturn(ImmutableSet
        .of(new SystemStreamPartition("insystem", "instream", new Partition(0)),
            new SystemStreamPartition("insystem2", "instream2", new Partition(0))));
    Config config = mock(Config.class);

    StreamGraphBuilder sgb = new TestStreamGraphBuilder();
    sot = new StreamOperatorTask(sgb);
    sot.init(config, taskContext);
  }

  @Test
  public void join() {
    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream with same keys
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(outputSum, 110);
  }

  @Test
  public void joinReverse() {
    // push messages to second stream
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to first stream with same keys
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(outputSum, 110);
  }

  @Test
  public void joinNoMatch() {
    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream with different keys
    numbers.forEach(n -> sot.process(new SecondStreamIME(n + 100, n), messageCollector, taskCoordinator));

    assertTrue(output.isEmpty());
  }

  @Test
  public void joinNoMatchReverse() {
    // push messages to second stream
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to first stream with different keys
    numbers.forEach(n -> sot.process(new FirstStreamIME(n + 100, n), messageCollector, taskCoordinator));

    assertTrue(output.isEmpty());
  }

  @Test
  public void joinRetainsLatestMessageForKey() {
    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to first stream again with same keys but different values
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, 2 * n), messageCollector, taskCoordinator));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(outputSum, 165); // should use latest messages in the first stream
  }

  @Test
  public void joinRetainsLatestMessageForKeyReverse() {
    // push messages to second stream
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream again with same keys but different values
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, 2 * n), messageCollector, taskCoordinator));
    // push messages to first stream with same key
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(outputSum, 165); // should use latest messages in the second stream
  }

  @Test
  public void joinRetainsMatchedMessages() {
    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(outputSum, 110);

    output.clear();

    // push messages to first stream with same keys once again.
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    int newOutputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(newOutputSum, 110); // should produce the same output as before
  }

  @Test
  public void joinRetainsMatchedMessagesReverse() {
    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(outputSum, 110);

    output.clear();

    // push messages to second stream with same keys once again.
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));
    int newOutputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(newOutputSum, 110); // should produce the same output as before
  }

  private class TestStreamGraphBuilder implements StreamGraphBuilder {
    StreamSpec inStreamSpec = new StreamSpec() {
      @Override
      public SystemStream getSystemStream() {
        return new SystemStream("insystem", "instream");
      }

      @Override
      public Properties getProperties() {
        return null;
      }
    };

    StreamSpec inStreamSpec2 = new StreamSpec() {
      @Override
      public SystemStream getSystemStream() {
        return new SystemStream("insystem2", "instream2");
      }

      @Override
      public Properties getProperties() {
        return null;
      }
    };

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<MessageEnvelope<Integer, Integer>> inStream = graph.createInStream(inStreamSpec, null, null);
      MessageStream<MessageEnvelope<Integer, Integer>> inStream2 = graph.createInStream(inStreamSpec2, null, null);

      inStream
          .join(inStream2, new TestJoinFunction())
          .map(m -> {
              output.add(m);
              return m;
            });
    }
  }

  private class TestJoinFunction
      implements JoinFunction<Integer, MessageEnvelope<Integer, Integer>, MessageEnvelope<Integer, Integer>, Integer> {
    @Override
    public Integer apply(MessageEnvelope<Integer, Integer> message,
        MessageEnvelope<Integer, Integer> otherMessage) {
      return message.getMessage() + otherMessage.getMessage();
    }

    @Override
    public Integer getFirstKey(MessageEnvelope<Integer, Integer> message) {
      return message.getKey();
    }

    @Override
    public Integer getSecondKey(MessageEnvelope<Integer, Integer> message) {
      return message.getKey();
    }
  }

  private class FirstStreamIME extends IncomingMessageEnvelope {
    FirstStreamIME(Integer key, Integer message) {
      super(new SystemStreamPartition("insystem", "instream", new Partition(0)), "1", key, message);
    }
  }

  private class SecondStreamIME extends IncomingMessageEnvelope {
    SecondStreamIME(Integer key, Integer message) {
      super(new SystemStreamPartition("insystem2", "instream2", new Partition(0)), "1", key, message);
    }
  }
}
