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
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.context.MockContext;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.impl.store.TestInMemoryStore;
import org.apache.samza.operators.impl.store.TimestampedValueSerde;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.testUtils.StreamTestUtils;
import org.apache.samza.testUtils.TestClock;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestJoinOperator {
  private static final Duration JOIN_TTL = Duration.ofMinutes(10);

  private final TaskCoordinator taskCoordinator = mock(TaskCoordinator.class);
  private final TaskCallback taskCallback = mock(TaskCallback.class);
  private final Set<Integer> numbers = ImmutableSet.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

  @Test
  public void join() throws Exception {
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    // push messages to second stream with same keys
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, outputSum);
  }

  @Test(expected = SamzaException.class)
  public void joinWithSelfThrowsException() throws Exception {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put("job.name", "jobName");
    mapConfig.put("job.id", "jobId");
    StreamTestUtils.addStreamConfigs(mapConfig, "inStream", "insystem", "instream");
    Config config = new MapConfig(mapConfig);

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(appDesc -> {
      IntegerSerde integerSerde = new IntegerSerde();
      KVSerde<Integer, Integer> kvSerde = KVSerde.of(integerSerde, integerSerde);
      GenericSystemDescriptor sd = new GenericSystemDescriptor("insystem", "mockFactoryClassName");
      GenericInputDescriptor<KV<Integer, Integer>> inputDescriptor = sd.getInputDescriptor("inStream", kvSerde);

      MessageStream<KV<Integer, Integer>> inStream = appDesc.getInputStream(inputDescriptor);

      inStream.join(inStream, new TestJoinFunction(), integerSerde, kvSerde, kvSerde, JOIN_TTL, "join");
    }, config);

    createStreamOperatorTask(SystemClock.instance(), streamAppDesc); // should throw an exception
  }

  @Test
  public void joinFnInitAndClose() throws Exception {
    TestJoinFunction joinFn = new TestJoinFunction();
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(joinFn);
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);

    MessageCollector messageCollector = mock(MessageCollector.class);

    // push messages to first stream
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    // close should not be called till now
    sot.close();

    verify(messageCollector, times(0)).send(any(OutgoingMessageEnvelope.class));
    // Make sure the joinFn has been copied instead of directly referred by the task instance
    assertEquals(0, joinFn.getNumInitCalls());
    assertEquals(0, joinFn.getNumCloseCalls());
  }

  @Test
  public void joinReverse() throws Exception {
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to second stream
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    // push messages to first stream with same keys
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, outputSum);
  }

  @Test
  public void joinNoMatch() throws Exception {
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    // push messages to second stream with different keys
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n + 100, n), messageCollector, taskCoordinator, taskCallback));

    assertTrue(output.isEmpty());
  }

  @Test
  public void joinNoMatchReverse() throws Exception {
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to second stream
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    // push messages to first stream with different keys
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n + 100, n), messageCollector, taskCoordinator, taskCallback));

    assertTrue(output.isEmpty());
  }

  @Test
  public void joinRetainsLatestMessageForKey() throws Exception {
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    // push messages to first stream again with same keys but different values
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, 2 * n), messageCollector, taskCoordinator, taskCallback));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(165, outputSum); // should use latest messages in the first stream
  }

  @Test
  public void joinRetainsLatestMessageForKeyReverse() throws Exception {
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to second stream
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    // push messages to second stream again with same keys but different values
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, 2 * n), messageCollector, taskCoordinator, taskCallback));
    // push messages to first stream with same key
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(165, outputSum); // should use latest messages in the second stream
  }

  @Test
  public void joinRetainsMatchedMessages() throws Exception {
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, outputSum);

    output.clear();

    // push messages to first stream with same keys once again.
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    int newOutputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, newOutputSum); // should produce the same output as before
  }

  @Test
  public void joinRetainsMatchedMessagesReverse() throws Exception {
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(SystemClock.instance(), streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    // push messages to second stream with same key
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    int outputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, outputSum);

    output.clear();

    // push messages to second stream with same keys once again.
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));
    int newOutputSum = output.stream().reduce(0, (s, m) -> s + m);
    assertEquals(110, newOutputSum); // should produce the same output as before
  }

  @Test
  public void joinRemovesExpiredMessages() throws Exception {
    TestClock testClock = new TestClock();
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(testClock, streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to first stream
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    testClock.advanceTime(JOIN_TTL.plus(Duration.ofMinutes(1))); // 1 minute after ttl
    sot.window(messageCollector, taskCoordinator); // should expire first stream messages

    // push messages to second stream with same key
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    assertTrue(output.isEmpty());
  }

  @Test
  public void joinRemovesExpiredMessagesReverse() throws Exception {
    TestClock testClock = new TestClock();
    StreamApplicationDescriptorImpl streamAppDesc = this.getTestJoinStreamGraph(new TestJoinFunction());
    StreamOperatorTask sot = createStreamOperatorTask(testClock, streamAppDesc);
    List<Integer> output = new ArrayList<>();
    MessageCollector messageCollector = envelope -> output.add((Integer) envelope.getMessage());

    // push messages to second stream
    numbers.forEach(n -> sot.processAsync(new SecondStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    testClock.advanceTime(JOIN_TTL.plus(Duration.ofMinutes(1))); // 1 minute after ttl
    sot.window(messageCollector, taskCoordinator); // should expire second stream messages

    // push messages to first stream with same key
    numbers.forEach(n -> sot.processAsync(new FirstStreamIME(n, n), messageCollector, taskCoordinator, taskCallback));

    assertTrue(output.isEmpty());
  }

  private StreamOperatorTask createStreamOperatorTask(Clock clock, StreamApplicationDescriptorImpl graphSpec)
      throws Exception {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put("job.name", "jobName");
    mapConfig.put("job.id", "jobId");
    StreamTestUtils.addStreamConfigs(mapConfig, "inStream", "insystem", "instream");
    StreamTestUtils.addStreamConfigs(mapConfig, "inStream2", "insystem", "instream2");
    Context context = new MockContext(new MapConfig(mapConfig));
    TaskModel taskModel = mock(TaskModel.class);
    when(taskModel.getSystemStreamPartitions()).thenReturn(ImmutableSet
        .of(new SystemStreamPartition("insystem", "instream", new Partition(0)),
            new SystemStreamPartition("insystem", "instream2", new Partition(0))));
    when(context.getTaskContext().getTaskModel()).thenReturn(taskModel);
    when(context.getTaskContext().getTaskMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    when(context.getContainerContext().getContainerMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    // need to return different stores for left and right side
    IntegerSerde integerSerde = new IntegerSerde();
    TimestampedValueSerde timestampedValueSerde = new TimestampedValueSerde(new KVSerde(integerSerde, integerSerde));
    when(context.getTaskContext().getStore(eq("jobName-jobId-join-j1-L")))
        .thenReturn(new TestInMemoryStore(integerSerde, timestampedValueSerde));
    when(context.getTaskContext().getStore(eq("jobName-jobId-join-j1-R")))
        .thenReturn(new TestInMemoryStore(integerSerde, timestampedValueSerde));

    StreamOperatorTask sot = new StreamOperatorTask(graphSpec.getOperatorSpecGraph(), clock);
    sot.init(context);
    return sot;
  }

  private StreamApplicationDescriptorImpl getTestJoinStreamGraph(TestJoinFunction joinFn) throws IOException {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put("job.name", "jobName");
    mapConfig.put("job.id", "jobId");
    StreamTestUtils.addStreamConfigs(mapConfig, "inStream", "insystem", "instream");
    StreamTestUtils.addStreamConfigs(mapConfig, "inStream2", "insystem", "instream2");
    Config config = new MapConfig(mapConfig);

    return new StreamApplicationDescriptorImpl(appDesc -> {
      IntegerSerde integerSerde = new IntegerSerde();
      KVSerde<Integer, Integer> kvSerde = KVSerde.of(integerSerde, integerSerde);
      GenericSystemDescriptor sd = new GenericSystemDescriptor("insystem", "mockFactoryClassName");
      GenericInputDescriptor<KV<Integer, Integer>> inputDescriptor1 = sd.getInputDescriptor("inStream", kvSerde);
      GenericInputDescriptor<KV<Integer, Integer>> inputDescriptor2 = sd.getInputDescriptor("inStream2", kvSerde);

      MessageStream<KV<Integer, Integer>> inStream = appDesc.getInputStream(inputDescriptor1);
      MessageStream<KV<Integer, Integer>> inStream2 = appDesc.getInputStream(inputDescriptor2);

      inStream
          .join(inStream2, joinFn, integerSerde, kvSerde, kvSerde, JOIN_TTL, "j1")
          .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });
    }, config);
  }

  private static class TestJoinFunction
      implements JoinFunction<Integer, KV<Integer, Integer>, KV<Integer, Integer>, Integer> {

    private int numInitCalls = 0;
    private int numCloseCalls = 0;

    @Override
    public void init(Context context) {
      numInitCalls++;
    }

    @Override
    public Integer apply(KV<Integer, Integer> message, KV<Integer, Integer> otherMessage) {
      return message.value + otherMessage.value;
    }

    @Override
    public Integer getFirstKey(KV<Integer, Integer> message) {
      return message.key;
    }

    @Override
    public Integer getSecondKey(KV<Integer, Integer> message) {
      return message.key;
    }

    @Override
    public void close() {
      numCloseCalls++;
    }

    int getNumInitCalls() {
      return numInitCalls;
    }

    int getNumCloseCalls() {
      return numCloseCalls;
    }
  }

  private static class FirstStreamIME extends IncomingMessageEnvelope {
    FirstStreamIME(Integer key, Integer value) {
      super(new SystemStreamPartition("insystem", "instream", new Partition(0)), "1", key, value);
    }
  }

  private static class SecondStreamIME extends IncomingMessageEnvelope {
    SecondStreamIME(Integer key, Integer value) {
      super(new SystemStreamPartition("insystem", "instream2", new Partition(0)), "1", key, value);
    }
  }
}
