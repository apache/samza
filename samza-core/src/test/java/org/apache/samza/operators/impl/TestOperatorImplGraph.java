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

package org.apache.samza.operators.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.testUtils.StreamTestUtils;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.TimestampedValue;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class TestOperatorImplGraph {

  private void addOperatorRecursively(HashSet<OperatorImpl> s, OperatorImpl op) {
    List<OperatorImpl> operators = new ArrayList<>();
    operators.add(op);
    while (!operators.isEmpty()) {
      OperatorImpl opImpl = operators.remove(0);
      s.add(opImpl);
      if (!opImpl.registeredOperators.isEmpty()) {
        operators.addAll(opImpl.registeredOperators);
      }
    }
  }

  static class TestMapFunction<M, OM> extends BaseTestFunction implements MapFunction<M, OM> {
    final Function<M, OM> mapFn;

    public TestMapFunction(String opId, Function<M, OM> mapFn) {
      super(opId);
      this.mapFn = mapFn;
    }

    @Override
    public OM apply(M message) {
      return this.mapFn.apply(message);
    }
  }

  static class TestJoinFunction<K, M, JM, RM> extends BaseTestFunction implements JoinFunction<K, M, JM, RM> {
    final BiFunction<M, JM, RM> joiner;
    final Function<M, K> firstKeyFn;
    final Function<JM, K> secondKeyFn;
    final Collection<RM> joinResults = new HashSet<>();

    public TestJoinFunction(String opId, BiFunction<M, JM, RM> joiner, Function<M, K> firstKeyFn, Function<JM, K> secondKeyFn) {
      super(opId);
      this.joiner = joiner;
      this.firstKeyFn = firstKeyFn;
      this.secondKeyFn = secondKeyFn;
    }

    @Override
    public RM apply(M message, JM otherMessage) {
      RM result = this.joiner.apply(message, otherMessage);
      this.joinResults.add(result);
      return result;
    }

    @Override
    public K getFirstKey(M message) {
      return this.firstKeyFn.apply(message);
    }

    @Override
    public K getSecondKey(JM message) {
      return this.secondKeyFn.apply(message);
    }
  }

  static abstract class BaseTestFunction implements InitableFunction, ClosableFunction, Serializable {

    static Map<TaskName, Map<String, BaseTestFunction>> perTaskFunctionMap = new HashMap<>();
    static Map<TaskName, List<String>> perTaskInitList = new HashMap<>();
    static Map<TaskName, List<String>> perTaskCloseList = new HashMap<>();
    int numInitCalled = 0;
    int numCloseCalled = 0;
    TaskName taskName = null;
    final String opId;

    public BaseTestFunction(String opId) {
      this.opId = opId;
    }

    static public void reset() {
      perTaskFunctionMap.clear();
      perTaskCloseList.clear();
      perTaskInitList.clear();
    }

    static public BaseTestFunction getInstanceByTaskName(TaskName taskName, String opId) {
      return perTaskFunctionMap.get(taskName).get(opId);
    }

    static public List<String> getInitListByTaskName(TaskName taskName) {
      return perTaskInitList.get(taskName);
    }

    static public List<String> getCloseListByTaskName(TaskName taskName) {
      return perTaskCloseList.get(taskName);
    }

    @Override
    public void close() {
      if (this.taskName == null) {
        throw new IllegalStateException("Close called before bootstrap");
      }
      if (perTaskFunctionMap.get(this.taskName) == null || !perTaskFunctionMap.get(this.taskName).containsKey(opId)) {
        throw new IllegalStateException("Close called before bootstrap");
      }

      if (perTaskCloseList.get(this.taskName) == null) {
        perTaskCloseList.put(taskName, new ArrayList<String>() { { this.add(opId); } });
      } else {
        perTaskCloseList.get(taskName).add(opId);
      }

      this.numCloseCalled++;
    }

    @Override
    public void init(Config config, TaskContext context) {
      if (perTaskFunctionMap.get(context.getTaskName()) == null) {
        perTaskFunctionMap.put(context.getTaskName(), new HashMap<String, BaseTestFunction>() { { this.put(opId, BaseTestFunction.this); } });
      } else {
        if (perTaskFunctionMap.get(context.getTaskName()).containsKey(opId)) {
          throw new IllegalStateException(String.format("Multiple bootstrap called for op %s in the same task instance %s", opId, this.taskName.getTaskName()));
        }
        perTaskFunctionMap.get(context.getTaskName()).put(opId, this);
      }
      if (perTaskInitList.get(context.getTaskName()) == null) {
        perTaskInitList.put(context.getTaskName(), new ArrayList<String>() { { this.add(opId); } });
      } else {
        perTaskInitList.get(context.getTaskName()).add(opId);
      }
      this.taskName = context.getTaskName();
      this.numInitCalled++;
    }
  }

  @After
  public void tearDown() {
    BaseTestFunction.reset();
  }

  @Test
  public void testEmptyChain() {
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));
    OperatorImplGraph opGraph =
        new OperatorImplGraph(graphSpec.getOperatorSpecGraph(), mock(Config.class), mock(TaskContextImpl.class), mock(Clock.class));
    assertEquals(0, opGraph.getAllInputOperators().size());
  }

  @Test
  public void testLinearChain() {
    String inputStreamId = "input";
    String inputSystem = "input-system";
    String inputPhysicalName = "input-stream";
    String outputStreamId = "output";
    String outputSystem = "output-system";
    String outputPhysicalName = "output-stream";
    String intermediateSystem = "intermediate-system";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "jobName");
    configs.put(JobConfig.JOB_ID(), "jobId");
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), intermediateSystem);
    StreamTestUtils.addStreamConfigs(configs, inputStreamId, inputSystem, inputPhysicalName);
    StreamTestUtils.addStreamConfigs(configs, outputStreamId, outputSystem, outputPhysicalName);
    Config config = new MapConfig(configs);

    StreamGraphSpec graphSpec = new StreamGraphSpec(config);

    MessageStream<Object> inputStream = graphSpec.getInputStream(inputStreamId);
    OutputStream<Object> outputStream = graphSpec.getOutputStream(outputStreamId);

    inputStream
        .filter(mock(FilterFunction.class))
        .map(mock(MapFunction.class))
        .sendTo(outputStream);

    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    when(mockTaskContext.getTaskName()).thenReturn(new TaskName("task 0"));
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(graphSpec.getOperatorSpecGraph(), config, mockTaskContext, mock(Clock.class));

    InputOperatorImpl inputOpImpl = opImplGraph.getInputOperator(new SystemStream(inputSystem, inputPhysicalName));
    assertEquals(1, inputOpImpl.registeredOperators.size());

    OperatorImpl filterOpImpl = (StreamOperatorImpl) inputOpImpl.registeredOperators.iterator().next();
    assertEquals(1, filterOpImpl.registeredOperators.size());
    assertEquals(OpCode.FILTER, filterOpImpl.getOperatorSpec().getOpCode());

    OperatorImpl mapOpImpl = (StreamOperatorImpl) filterOpImpl.registeredOperators.iterator().next();
    assertEquals(1, mapOpImpl.registeredOperators.size());
    assertEquals(OpCode.MAP, mapOpImpl.getOperatorSpec().getOpCode());

    OperatorImpl sendToOpImpl = (OutputOperatorImpl) mapOpImpl.registeredOperators.iterator().next();
    assertEquals(0, sendToOpImpl.registeredOperators.size());
    assertEquals(OpCode.SEND_TO, sendToOpImpl.getOperatorSpec().getOpCode());
  }

  @Test
  public void testPartitionByChain() {
    String inputStreamId = "input";
    String inputSystem = "input-system";
    String inputPhysicalName = "input-stream";
    String outputStreamId = "output";
    String outputSystem = "output-system";
    String outputPhysicalName = "output-stream";
    String intermediateStreamId = "jobName-jobId-partition_by-p1";
    String intermediateSystem = "intermediate-system";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "jobName");
    configs.put(JobConfig.JOB_ID(), "jobId");
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), intermediateSystem);
    StreamTestUtils.addStreamConfigs(configs, inputStreamId, inputSystem, inputPhysicalName);
    StreamTestUtils.addStreamConfigs(configs, outputStreamId, outputSystem, outputPhysicalName);
    Config config = new MapConfig(configs);

    StreamGraphSpec graphSpec = new StreamGraphSpec(config);
    MessageStream<Object> inputStream = graphSpec.getInputStream(inputStreamId);
    OutputStream<KV<Integer, String>> outputStream = graphSpec
        .getOutputStream(outputStreamId, KVSerde.of(mock(IntegerSerde.class), mock(StringSerde.class)));

    inputStream
        .partitionBy(Object::hashCode, Object::toString,
            KVSerde.of(mock(IntegerSerde.class), mock(StringSerde.class)), "p1")
        .sendTo(outputStream);

    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    when(mockTaskContext.getTaskName()).thenReturn(new TaskName("task 0"));
    JobModel jobModel = mock(JobModel.class);
    ContainerModel containerModel = mock(ContainerModel.class);
    TaskModel taskModel = mock(TaskModel.class);
    when(jobModel.getContainers()).thenReturn(Collections.singletonMap("0", containerModel));
    when(containerModel.getTasks()).thenReturn(Collections.singletonMap(new TaskName("task 0"), taskModel));
    when(taskModel.getSystemStreamPartitions()).thenReturn(Collections.emptySet());
    when(mockTaskContext.getJobModel()).thenReturn(jobModel);
    SamzaContainerContext containerContext =
        new SamzaContainerContext("0", config, Collections.singleton(new TaskName("task 0")), new MetricsRegistryMap());
    when(mockTaskContext.getSamzaContainerContext()).thenReturn(containerContext);
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(graphSpec.getOperatorSpecGraph(), config, mockTaskContext, mock(Clock.class));

    InputOperatorImpl inputOpImpl = opImplGraph.getInputOperator(new SystemStream(inputSystem, inputPhysicalName));
    assertEquals(1, inputOpImpl.registeredOperators.size());

    OperatorImpl partitionByOpImpl = (PartitionByOperatorImpl) inputOpImpl.registeredOperators.iterator().next();
    assertEquals(0, partitionByOpImpl.registeredOperators.size()); // is terminal but paired with an input operator
    assertEquals(OpCode.PARTITION_BY, partitionByOpImpl.getOperatorSpec().getOpCode());

    InputOperatorImpl repartitionedInputOpImpl =
        opImplGraph.getInputOperator(new SystemStream(intermediateSystem, intermediateStreamId));
    assertEquals(1, repartitionedInputOpImpl.registeredOperators.size());

    OperatorImpl sendToOpImpl = (OutputOperatorImpl) repartitionedInputOpImpl.registeredOperators.iterator().next();
    assertEquals(0, sendToOpImpl.registeredOperators.size());
    assertEquals(OpCode.SEND_TO, sendToOpImpl.getOperatorSpec().getOpCode());
  }

  @Test
  public void testBroadcastChain() {
    String inputStreamId = "input";
    HashMap<String, String> configMap = new HashMap<>();
    StreamTestUtils.addStreamConfigs(configMap, "input", "input-system", "input-stream");
    Config config = new MapConfig(configMap);
    StreamGraphSpec graphSpec = new StreamGraphSpec(config);

    MessageStream<Object> inputStream = graphSpec.getInputStream(inputStreamId);
    inputStream.filter(mock(FilterFunction.class));
    inputStream.map(mock(MapFunction.class));

    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(graphSpec.getOperatorSpecGraph(), config, mockTaskContext, mock(Clock.class));

    InputOperatorImpl inputOpImpl = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream"));
    assertEquals(2, inputOpImpl.registeredOperators.size());
    assertTrue(inputOpImpl.registeredOperators.stream()
        .anyMatch(opImpl -> ((OperatorImpl) opImpl).getOperatorSpec().getOpCode() == OpCode.FILTER));
    assertTrue(inputOpImpl.registeredOperators.stream()
        .anyMatch(opImpl -> ((OperatorImpl) opImpl).getOperatorSpec().getOpCode() == OpCode.MAP));
  }

  @Test
  public void testMergeChain() {
    String inputStreamId = "input";
    StreamGraphSpec graphSpec = new StreamGraphSpec(mock(Config.class));

    MessageStream<Object> inputStream = graphSpec.getInputStream(inputStreamId);
    MessageStream<Object> stream1 = inputStream.filter(mock(FilterFunction.class));
    MessageStream<Object> stream2 = inputStream.map(mock(MapFunction.class));
    MessageStream<Object> mergedStream = stream1.merge(Collections.singleton(stream2));

    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    TaskName mockTaskName = mock(TaskName.class);
    when(mockTaskContext.getTaskName()).thenReturn(mockTaskName);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());

    MapFunction testMapFunction = new TestMapFunction<Object, Object>("test-map-1", (Function & Serializable) m -> m);
    mergedStream.map(testMapFunction);

    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(graphSpec.getOperatorSpecGraph(), mock(Config.class), mockTaskContext, mock(Clock.class));

    Set<OperatorImpl> opSet = opImplGraph.getAllInputOperators().stream().collect(HashSet::new,
        (s, op) -> addOperatorRecursively(s, op), HashSet::addAll);
    Object[] mergeOps = opSet.stream().filter(op -> op.getOperatorSpec().getOpCode() == OpCode.MERGE).toArray();
    assertEquals(mergeOps.length, 1);
    assertEquals(((OperatorImpl) mergeOps[0]).registeredOperators.size(), 1);
    OperatorImpl mapOp = (OperatorImpl) ((OperatorImpl) mergeOps[0]).registeredOperators.iterator().next();
    assertEquals(mapOp.getOperatorSpec().getOpCode(), OpCode.MAP);

    // verify that the DAG after merge is only traversed & initialized once
    assertEquals(TestMapFunction.getInstanceByTaskName(mockTaskName, "test-map-1").numInitCalled, 1);
  }

  @Test
  public void testJoinChain() {
    String inputStreamId1 = "input1";
    String inputStreamId2 = "input2";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "jobName");
    configs.put(JobConfig.JOB_ID(), "jobId");
    StreamTestUtils.addStreamConfigs(configs, "input1", "input-system", "input-stream1");
    StreamTestUtils.addStreamConfigs(configs, "input2", "input-system", "input-stream2");
    Config config = new MapConfig(configs);
    StreamGraphSpec graphSpec = new StreamGraphSpec(config);

    Integer joinKey = new Integer(1);
    Function<Object, Integer> keyFn = (Function & Serializable) m -> joinKey;
    JoinFunction testJoinFunction = new TestJoinFunction("jobName-jobId-join-j1",
        (BiFunction & Serializable) (m1, m2) -> KV.of(m1, m2), keyFn, keyFn);
    MessageStream<Object> inputStream1 = graphSpec.getInputStream(inputStreamId1, new NoOpSerde<>());
    MessageStream<Object> inputStream2 = graphSpec.getInputStream(inputStreamId2, new NoOpSerde<>());
    inputStream1.join(inputStream2, testJoinFunction,
        mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j1");

    TaskName mockTaskName = mock(TaskName.class);
    TaskContextImpl mockTaskContext = mock(TaskContextImpl.class);
    when(mockTaskContext.getTaskName()).thenReturn(mockTaskName);
    when(mockTaskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    KeyValueStore mockLeftStore = mock(KeyValueStore.class);
    when(mockTaskContext.getStore(eq("jobName-jobId-join-j1-L"))).thenReturn(mockLeftStore);
    KeyValueStore mockRightStore = mock(KeyValueStore.class);
    when(mockTaskContext.getStore(eq("jobName-jobId-join-j1-R"))).thenReturn(mockRightStore);
    OperatorImplGraph opImplGraph =
        new OperatorImplGraph(graphSpec.getOperatorSpecGraph(), config, mockTaskContext, mock(Clock.class));

    // verify that join function is initialized once.
    assertEquals(TestJoinFunction.getInstanceByTaskName(mockTaskName, "jobName-jobId-join-j1").numInitCalled, 1);

    InputOperatorImpl inputOpImpl1 = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream1"));
    InputOperatorImpl inputOpImpl2 = opImplGraph.getInputOperator(new SystemStream("input-system", "input-stream2"));
    PartialJoinOperatorImpl leftPartialJoinOpImpl =
        (PartialJoinOperatorImpl) inputOpImpl1.registeredOperators.iterator().next();
    PartialJoinOperatorImpl rightPartialJoinOpImpl =
        (PartialJoinOperatorImpl) inputOpImpl2.registeredOperators.iterator().next();

    assertEquals(leftPartialJoinOpImpl.getOperatorSpec(), rightPartialJoinOpImpl.getOperatorSpec());
    assertNotSame(leftPartialJoinOpImpl, rightPartialJoinOpImpl);

    // verify that left partial join operator calls getFirstKey
    Object mockLeftMessage = mock(Object.class);
    long currentTimeMillis = System.currentTimeMillis();
    when(mockLeftStore.get(eq(joinKey))).thenReturn(new TimestampedValue<>(mockLeftMessage, currentTimeMillis));
    inputOpImpl1.onMessage(KV.of("", mockLeftMessage), mock(MessageCollector.class), mock(TaskCoordinator.class));

    // verify that right partial join operator calls getSecondKey
    Object mockRightMessage = mock(Object.class);
    when(mockRightStore.get(eq(joinKey))).thenReturn(new TimestampedValue<>(mockRightMessage, currentTimeMillis));
    inputOpImpl2.onMessage(KV.of("", mockRightMessage), mock(MessageCollector.class), mock(TaskCoordinator.class));


    // verify that the join function apply is called with the correct messages on match
    assertEquals(((TestJoinFunction) TestJoinFunction.getInstanceByTaskName(mockTaskName, "jobName-jobId-join-j1")).joinResults.size(), 1);
    KV joinResult = (KV) ((TestJoinFunction) TestJoinFunction.getInstanceByTaskName(mockTaskName, "jobName-jobId-join-j1")).joinResults.iterator().next();
    assertEquals(joinResult.getKey(), mockLeftMessage);
    assertEquals(joinResult.getValue(), mockRightMessage);
  }

  @Test
  public void testOperatorGraphInitAndClose() {
    String inputStreamId1 = "input1";
    String inputStreamId2 = "input2";
    Config mockConfig = mock(Config.class);
    TaskName mockTaskName = mock(TaskName.class);
    TaskContextImpl mockContext = mock(TaskContextImpl.class);
    when(mockContext.getTaskName()).thenReturn(mockTaskName);
    when(mockContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    StreamGraphSpec graphSpec = new StreamGraphSpec(mockConfig);

    MessageStream<Object> inputStream1 = graphSpec.getInputStream(inputStreamId1);
    MessageStream<Object> inputStream2 = graphSpec.getInputStream(inputStreamId2);

    Function mapFn = (Function & Serializable) m -> m;
    inputStream1.map(new TestMapFunction<Object, Object>("1", mapFn))
        .map(new TestMapFunction<Object, Object>("2", mapFn));

    inputStream2.map(new TestMapFunction<Object, Object>("3", mapFn))
        .map(new TestMapFunction<Object, Object>("4", mapFn));

    OperatorImplGraph opImplGraph = new OperatorImplGraph(graphSpec.getOperatorSpecGraph(), mockConfig, mockContext, SystemClock.instance());

    List<String> initializedOperators = BaseTestFunction.getInitListByTaskName(mockTaskName);

    // Assert that initialization occurs in topological order.
    assertEquals(initializedOperators.get(0), "1");
    assertEquals(initializedOperators.get(1), "2");
    assertEquals(initializedOperators.get(2), "3");
    assertEquals(initializedOperators.get(3), "4");

    // Assert that finalization occurs in reverse topological order.
    opImplGraph.close();
    List<String> closedOperators = BaseTestFunction.getCloseListByTaskName(mockTaskName);
    assertEquals(closedOperators.get(0), "4");
    assertEquals(closedOperators.get(1), "3");
    assertEquals(closedOperators.get(2), "2");
    assertEquals(closedOperators.get(3), "1");
  }

  @Test
  public void testGetStreamToConsumerTasks() {
    String system = "test-system";
    String streamId0 = "test-stream-0";
    String streamId1 = "test-stream-1";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "test-app");
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), "test-system");
    StreamTestUtils.addStreamConfigs(configs, streamId0, system, streamId0);
    StreamTestUtils.addStreamConfigs(configs, streamId1, system, streamId1);
    Config config = new MapConfig(configs);

    SystemStreamPartition ssp0 = new SystemStreamPartition(system, streamId0, new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition(system, streamId0, new Partition(1));
    SystemStreamPartition ssp2 = new SystemStreamPartition(system, streamId1, new Partition(0));

    TaskName task0 = new TaskName("Task 0");
    TaskName task1 = new TaskName("Task 1");
    Set<SystemStreamPartition> ssps = new HashSet<>();
    ssps.add(ssp0);
    ssps.add(ssp2);
    TaskModel tm0 = new TaskModel(task0, ssps, new Partition(0));
    ContainerModel cm0 = new ContainerModel("c0", 0, Collections.singletonMap(task0, tm0));
    TaskModel tm1 = new TaskModel(task1, Collections.singleton(ssp1), new Partition(1));
    ContainerModel cm1 = new ContainerModel("c1", 1, Collections.singletonMap(task1, tm1));

    Map<String, ContainerModel> cms = new HashMap<>();
    cms.put(cm0.getProcessorId(), cm0);
    cms.put(cm1.getProcessorId(), cm1);

    JobModel jobModel = new JobModel(config, cms, null);
    Multimap<SystemStream, String> streamToTasks = OperatorImplGraph.getStreamToConsumerTasks(jobModel);
    assertEquals(streamToTasks.get(ssp0.getSystemStream()).size(), 2);
    assertEquals(streamToTasks.get(ssp2.getSystemStream()).size(), 1);
  }

  @Test
  public void testGetOutputToInputStreams() {
    String inputStreamId1 = "input1";
    String inputStreamId2 = "input2";
    String inputStreamId3 = "input3";
    String inputSystem = "input-system";

    String outputStreamId1 = "output1";
    String outputStreamId2 = "output2";
    String outputSystem = "output-system";

    String intStreamId1 = "test-app-1-partition_by-p1";
    String intStreamId2 = "test-app-1-partition_by-p2";
    String intSystem = "test-system";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "test-app");
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), intSystem);
    StreamTestUtils.addStreamConfigs(configs, inputStreamId1, inputSystem, inputStreamId1);
    StreamTestUtils.addStreamConfigs(configs, inputStreamId2, inputSystem, inputStreamId2);
    StreamTestUtils.addStreamConfigs(configs, inputStreamId3, inputSystem, inputStreamId3);
    StreamTestUtils.addStreamConfigs(configs, outputStreamId1, outputSystem, outputStreamId1);
    StreamTestUtils.addStreamConfigs(configs, outputStreamId2, outputSystem, outputStreamId2);
    Config config = new MapConfig(configs);

    StreamGraphSpec graphSpec = new StreamGraphSpec(config);
    MessageStream messageStream1 = graphSpec.getInputStream(inputStreamId1).map(m -> m);
    MessageStream messageStream2 = graphSpec.getInputStream(inputStreamId2).filter(m -> true);
    MessageStream messageStream3 =
        graphSpec.getInputStream(inputStreamId3)
            .filter(m -> true)
            .partitionBy(m -> "m", m -> m, "p1")
            .map(m -> m);
    OutputStream<Object> outputStream1 = graphSpec.getOutputStream(outputStreamId1);
    OutputStream<Object> outputStream2 = graphSpec.getOutputStream(outputStreamId2);

    messageStream1
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(2), "j1")
        .partitionBy(m -> "m", m -> m, "p2")
        .sendTo(outputStream1);
    messageStream3
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
        .sendTo(outputStream2);

    Multimap<SystemStream, SystemStream> outputToInput =
        OperatorImplGraph.getIntermediateToInputStreamsMap(graphSpec.getOperatorSpecGraph(), new StreamConfig(config));
    Collection<SystemStream> inputs = outputToInput.get(new SystemStream(intSystem, intStreamId2));
    assertEquals(inputs.size(), 2);
    assertTrue(inputs.contains(new SystemStream(inputSystem, inputStreamId1)));
    assertTrue(inputs.contains(new SystemStream(inputSystem, inputStreamId2)));

    inputs = outputToInput.get(new SystemStream(intSystem, intStreamId1));
    assertEquals(inputs.size(), 1);
    assertEquals(inputs.iterator().next(), new SystemStream(inputSystem, inputStreamId3));
  }

  @Test
  public void testGetProducerTaskCountForIntermediateStreams() {
    String inputStreamId1 = "input1";
    String inputStreamId2 = "input2";
    String inputStreamId3 = "input3";
    String inputSystem1 = "system1";
    String inputSystem2 = "system2";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "test-app");
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), inputSystem1);
    StreamTestUtils.addStreamConfigs(configs, inputStreamId1, inputSystem1, inputStreamId1);
    StreamTestUtils.addStreamConfigs(configs, inputStreamId2, inputSystem2, inputStreamId2);
    StreamTestUtils.addStreamConfigs(configs, inputStreamId3, inputSystem2, inputStreamId3);
    Config config = new MapConfig(configs);

    SystemStream input1 = new SystemStream("system1", "intput1");
    SystemStream input2 = new SystemStream("system2", "intput2");
    SystemStream input3 = new SystemStream("system2", "intput3");

    SystemStream int1 = new SystemStream("system1", "int1");
    SystemStream int2 = new SystemStream("system1", "int2");


    /**
     * the task assignment looks like the following:
     *
     * input1 -----> task0, task1 -----> int1
     *                                    ^
     * input2 ------> task1, task2--------|
     *                                    v
     * input3 ------> task1 -----------> int2
     *
     */
    String task0 = "Task 0";
    String task1 = "Task 1";
    String task2 = "Task 2";

    Multimap<SystemStream, String> streamToConsumerTasks = HashMultimap.create();
    streamToConsumerTasks.put(input1, task0);
    streamToConsumerTasks.put(input1, task1);
    streamToConsumerTasks.put(input2, task1);
    streamToConsumerTasks.put(input2, task2);
    streamToConsumerTasks.put(input3, task1);
    streamToConsumerTasks.put(int1, task0);
    streamToConsumerTasks.put(int1, task1);
    streamToConsumerTasks.put(int2, task0);

    Multimap<SystemStream, SystemStream> intermediateToInputStreams = HashMultimap.create();
    intermediateToInputStreams.put(int1, input1);
    intermediateToInputStreams.put(int1, input2);

    intermediateToInputStreams.put(int2, input2);
    intermediateToInputStreams.put(int2, input3);

    Map<SystemStream, Integer> counts = OperatorImplGraph.getProducerTaskCountForIntermediateStreams(
        streamToConsumerTasks, intermediateToInputStreams);
    assertTrue(counts.get(int1) == 3);
    assertTrue(counts.get(int2) == 2);
  }
}
