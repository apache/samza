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
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.testUtils.TestClock;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJoinOperator {
  private static final Duration JOIN_TTL = Duration.ofMinutes(10);

  private final TaskCoordinator taskCoordinator = mock(TaskCoordinator.class);
  private final Set<Integer> numbers = ImmutableSet.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

  @Test
  public void join() throws Exception {
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream with same keys
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, outputSum);
  }

  @Test
  public void testJoinFnInitAndClose() throws Exception {
    TestJoinFunction joinFn = new TestJoinFunction();
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(joinFn));
    assertEquals(1, joinFn.getNumInitCalls());
    MessageCollector messageCollector = mock(MessageCollector.class);

    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));

    // close should not be called till now
    assertEquals(0, joinFn.getNumCloseCalls());
    sot.close();

    // close should be called from sot.close()
    assertEquals(1, joinFn.getNumCloseCalls());
  }

  @Test
  public void joinReverse() throws Exception {
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to second stream
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to first stream with same keys
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, outputSum);
  }

  @Test
  public void joinNoMatch() throws Exception {
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream with different keys
    numbers.forEach(n -> sot.process(new SecondStreamIME(n + 100, n), messageCollector, taskCoordinator));

    assertTrue(output.isEmpty());
  }

  @Test
  public void joinNoMatchReverse() throws Exception {
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to second stream
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to first stream with different keys
    numbers.forEach(n -> sot.process(new FirstStreamIME(n + 100, n), messageCollector, taskCoordinator));

    assertTrue(output.isEmpty());
  }

  @Test
  public void joinRetainsLatestMessageForKey() throws Exception {
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to first stream again with same keys but different values
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, 2 * n), messageCollector, taskCoordinator));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(165, outputSum); // should use latest messages in the first stream
  }

  @Test
  public void joinRetainsLatestMessageForKeyReverse() throws Exception {
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to second stream
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream again with same keys but different values
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, 2 * n), messageCollector, taskCoordinator));
    // push messages to first stream with same key
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(165, outputSum); // should use latest messages in the second stream
  }

  @Test
  public void joinRetainsMatchedMessages() throws Exception {
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, outputSum);

    output.clear();

    // push messages to first stream with same keys once again.
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    int newOutputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, newOutputSum); // should produce the same output as before
  }

  @Test
  public void joinRetainsMatchedMessagesReverse() throws Exception {
    StreamOperatorTask sot = createStreamOperatorTask(new SystemClock(), new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, outputSum);

    output.clear();

    // push messages to second stream with same keys once again.
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));
    int newOutputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, newOutputSum); // should produce the same output as before
  }

  @Test
  public void joinRemovesExpiredMessages() throws Exception {
    TestClock testClock = new TestClock();
    StreamOperatorTask sot = createStreamOperatorTask(testClock, new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));

    testClock.advanceTime(JOIN_TTL.plus(Duration.ofMinutes(1))); // 1 minute after ttl
    sot.window(messageCollector, taskCoordinator); // should expire first stream messages

    // push messages to second stream with same key
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    assertTrue(output.isEmpty());
  }


  @Test
  public void joinRemovesExpiredMessagesReverse() throws Exception {
    TestClock testClock = new TestClock();
    StreamOperatorTask sot = createStreamOperatorTask(testClock, new TestJoinStreamApplication(new TestJoinFunction()));
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to second stream
    numbers.forEach(n -> sot.process(new SecondStreamIME(n, n), messageCollector, taskCoordinator));

    testClock.advanceTime(JOIN_TTL.plus(Duration.ofMinutes(1))); // 1 minute after ttl
    sot.window(messageCollector, taskCoordinator); // should expire second stream messages

    // push messages to first stream with same key
    numbers.forEach(n -> sot.process(new FirstStreamIME(n, n), messageCollector, taskCoordinator));

    assertTrue(output.isEmpty());
  }

  private StreamOperatorTask createStreamOperatorTask(Clock clock, StreamApplication app) throws Exception {
    ApplicationRunner runner = mock(ApplicationRunner.class);
    when(runner.getStreamSpec("instream")).thenReturn(new StreamSpec("instream", "instream", "insystem"));
    when(runner.getStreamSpec("instream2")).thenReturn(new StreamSpec("instream2", "instream2", "insystem2"));

    TaskContext taskContext = mock(TaskContext.class);
    when(taskContext.getSystemStreamPartitions()).thenReturn(ImmutableSet
        .of(new SystemStreamPartition("insystem", "instream", new Partition(0)),
            new SystemStreamPartition("insystem2", "instream2", new Partition(0))));
    when(taskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());

    Config config = mock(Config.class);

    StreamOperatorTask sot = new StreamOperatorTask(app, runner, clock);
    sot.init(config, taskContext);
    return sot;
  }

  private static class TestJoinStreamApplication implements StreamApplication {

    private final TestJoinFunction joinFn;

    TestJoinStreamApplication(TestJoinFunction joinFn) {
      this.joinFn = joinFn;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<FirstStreamIME> inStream =
          graph.getInputStream("instream", FirstStreamIME::new);
      MessageStream<SecondStreamIME> inStream2 =
          graph.getInputStream("instream2", SecondStreamIME::new);

      SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
      inStream
          .join(inStream2, joinFn, JOIN_TTL)
          .sink((message, messageCollector, taskCoordinator) -> {
              messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
            });
    }
  }

  private static class TestJoinFunction implements JoinFunction<Integer, FirstStreamIME, SecondStreamIME, Integer> {

    private int numInitCalls = 0;
    private int numCloseCalls = 0;

    @Override
    public void init(Config config, TaskContext context) {
      numInitCalls++;
    }

    @Override
    public Integer apply(FirstStreamIME message, SecondStreamIME otherMessage) {
      return (Integer) message.getMessage() + (Integer) otherMessage.getMessage();
    }

    @Override
    public Integer getFirstKey(FirstStreamIME message) {
      return (Integer) message.getKey();
    }

    @Override
    public Integer getSecondKey(SecondStreamIME message) {
      return (Integer) message.getKey();
    }

    @Override
    public void close() {
      numCloseCalls++;
    }

    public int getNumInitCalls() {
      return numInitCalls;
    }

    public int getNumCloseCalls() {
      return numCloseCalls;
    }
  }

  private static class FirstStreamIME extends IncomingMessageEnvelope {
    FirstStreamIME(Integer key, Integer message) {
      super(new SystemStreamPartition("insystem", "instream", new Partition(0)), "1", key, message);
    }
  }

  private static class SecondStreamIME extends IncomingMessageEnvelope {
    SecondStreamIME(Integer key, Integer message) {
      super(new SystemStreamPartition("insystem2", "instream2", new Partition(0)), "1", key, message);
    }
  }
}
