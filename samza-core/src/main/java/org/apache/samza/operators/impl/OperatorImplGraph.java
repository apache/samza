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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.samza.config.Config;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.impl.store.TimestampedValue;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.PartitionByOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.spec.SendToTableOperatorSpec;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * The DAG of {@link OperatorImpl}s corresponding to the DAG of {@link OperatorSpec}s.
 */
public class OperatorImplGraph {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorImplGraph.class);

  /**
   * A mapping from operator IDs to their {@link OperatorImpl}s in this graph. Used to avoid creating
   * multiple {@link OperatorImpl}s for an {@link OperatorSpec} when it's reached from different
   * {@link OperatorSpec}s during DAG traversals (e.g., for the merge operator).
   * We use a LHM for deterministic ordering in initializing and closing operators.
   */
  private final Map<String, OperatorImpl> operatorImpls = new LinkedHashMap<>();

  /**
   * A mapping from input {@link SystemStream}s to their {@link InputOperatorImpl} sub-DAG in this graph.
   */
  private final Map<SystemStream, InputOperatorImpl> inputOperators = new HashMap<>();

  /**
   * A mapping from {@link JoinOperatorSpec} IDs to their two {@link PartialJoinFunction}s. Used to associate
   * the two {@link PartialJoinOperatorImpl}s for a {@link JoinOperatorSpec} with each other since they're
   * reached from different {@link OperatorSpec} during DAG traversals.
   */
  private final Map<String, KV<PartialJoinFunction, PartialJoinFunction>> joinFunctions = new HashMap<>();

  private final Clock clock;

  /**
   * Constructs the DAG of {@link OperatorImpl}s corresponding to the the DAG of {@link OperatorSpec}s
   * in the {@code streamGraph}.
   *
   * @param streamGraph  the {@link StreamGraphImpl} containing the logical {@link OperatorSpec} DAG
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @param clock  the {@link Clock} to get current time
   */
  public OperatorImplGraph(StreamGraphImpl streamGraph, Config config, TaskContext context, Clock clock) {
    this.clock = clock;

    TaskContextImpl taskContext = (TaskContextImpl) context;
    Map<SystemStream, Integer> producerTaskCounts = hasIntermediateStreams(streamGraph) ?
        getProducerTaskCountForIntermediateStreams(getStreamToConsumerTasks(taskContext.getJobModel()),
            getIntermediateToInputStreamsMap(streamGraph)) :
        Collections.EMPTY_MAP;
    producerTaskCounts.forEach((stream, count) -> {
        LOG.info("{} has {} producer tasks.", stream, count);
      });

    // set states for end-of-stream
    taskContext.registerObject(EndOfStreamStates.class.getName(),
        new EndOfStreamStates(context.getSystemStreamPartitions(), producerTaskCounts));
    // set states for watermark
    taskContext.registerObject(WatermarkStates.class.getName(),
        new WatermarkStates(context.getSystemStreamPartitions(), producerTaskCounts));

    streamGraph.getInputOperators().forEach((streamSpec, inputOpSpec) -> {
        SystemStream systemStream = new SystemStream(streamSpec.getSystemName(), streamSpec.getPhysicalName());
        InputOperatorImpl inputOperatorImpl =
            (InputOperatorImpl) createAndRegisterOperatorImpl(null, inputOpSpec, systemStream, config, context);
        this.inputOperators.put(systemStream, inputOperatorImpl);
      });
  }

  /**
   * Get the {@link InputOperatorImpl} corresponding to the provided input {@code systemStream}.
   *
   * @param systemStream  input {@link SystemStream}
   * @return  the {@link InputOperatorImpl} that starts processing the input message
   */
  public InputOperatorImpl getInputOperator(SystemStream systemStream) {
    return this.inputOperators.get(systemStream);
  }

  public void close() {
    List<OperatorImpl> initializationOrder = new ArrayList<>(operatorImpls.values());
    List<OperatorImpl> finalizationOrder = Lists.reverse(initializationOrder);
    finalizationOrder.forEach(OperatorImpl::close);
  }

  /**
   * Get all {@link InputOperatorImpl}s for the graph.
   *
   * @return  an unmodifiable view of all {@link InputOperatorImpl}s for the graph
   */
  public Collection<InputOperatorImpl> getAllInputOperators() {
    return Collections.unmodifiableCollection(this.inputOperators.values());
  }

  /**
   * Traverses the DAG of {@link OperatorSpec}s starting from the provided {@link OperatorSpec},
   * creates the corresponding DAG of {@link OperatorImpl}s, and returns the root {@link OperatorImpl} node.
   *
   * @param prevOperatorSpec  the parent of the current {@code operatorSpec} in the traversal
   * @param operatorSpec  the operatorSpec to create the {@link OperatorImpl} for
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @return  the operator implementation for the operatorSpec
   */
  OperatorImpl createAndRegisterOperatorImpl(OperatorSpec prevOperatorSpec, OperatorSpec operatorSpec,
      SystemStream inputStream, Config config, TaskContext context) {
    if (!operatorImpls.containsKey(operatorSpec.getOpId()) || operatorSpec instanceof JoinOperatorSpec) {
      // Either this is the first time we've seen this operatorSpec, or this is a join operator spec
      // and we need to create 2 partial join operator impls for it. Initialize and register the sub-DAG.
      OperatorImpl operatorImpl = createOperatorImpl(prevOperatorSpec, operatorSpec, config, context);
      operatorImpl.init(config, context);
      operatorImpl.registerInputStream(inputStream);

      // Note: The key here is opImplId, which may not equal opId for some impls (e.g. PartialJoinOperatorImpl).
      // This is currently OK since we don't need to look up a partial join operator impl again during traversal
      // (a join cannot have a cycle).
      operatorImpls.put(operatorImpl.getOpImplId(), operatorImpl);

      Collection<OperatorSpec> registeredSpecs = operatorSpec.getRegisteredOperatorSpecs();
      registeredSpecs.forEach(registeredSpec -> {
          OperatorImpl nextImpl = createAndRegisterOperatorImpl(operatorSpec, registeredSpec, inputStream, config, context);
          operatorImpl.registerNextOperator(nextImpl);
        });
      return operatorImpl;
    } else {
      // the implementation corresponding to operatorSpec has already been instantiated
      // and registered, so we do not need to traverse the DAG further.
      return operatorImpls.get(operatorSpec.getOpId());
    }
  }

  /**
   * Creates a new {@link OperatorImpl} instance for the provided {@link OperatorSpec}.
   *
   * @param operatorSpec  the immutable {@link OperatorSpec} definition.
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @return  the {@link OperatorImpl} implementation instance
   */
  OperatorImpl createOperatorImpl(OperatorSpec prevOperatorSpec, OperatorSpec operatorSpec,
      Config config, TaskContext context) {
    if (operatorSpec instanceof InputOperatorSpec) {
      return new InputOperatorImpl((InputOperatorSpec) operatorSpec);
    } else if (operatorSpec instanceof StreamOperatorSpec) {
      return new StreamOperatorImpl((StreamOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof SinkOperatorSpec) {
      return new SinkOperatorImpl((SinkOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof OutputOperatorSpec) {
      return new OutputOperatorImpl((OutputOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof PartitionByOperatorSpec) {
      return new PartitionByOperatorImpl((PartitionByOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof WindowOperatorSpec) {
      return new WindowOperatorImpl((WindowOperatorSpec) operatorSpec, clock);
    } else if (operatorSpec instanceof JoinOperatorSpec) {
      return createPartialJoinOperatorImpl(prevOperatorSpec, (JoinOperatorSpec) operatorSpec, config, context, clock);
    } else if (operatorSpec instanceof StreamTableJoinOperatorSpec) {
      return new StreamTableJoinOperatorImpl((StreamTableJoinOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof SendToTableOperatorSpec) {
      return new SendToTableOperatorImpl((SendToTableOperatorSpec) operatorSpec, config, context);
    }
    throw new IllegalArgumentException(
        String.format("Unsupported OperatorSpec: %s", operatorSpec.getClass().getName()));
  }

  private PartialJoinOperatorImpl createPartialJoinOperatorImpl(OperatorSpec prevOperatorSpec,
      JoinOperatorSpec joinOpSpec, Config config, TaskContext context, Clock clock) {
    KV<PartialJoinFunction, PartialJoinFunction> partialJoinFunctions = getOrCreatePartialJoinFunctions(joinOpSpec);
    if (joinOpSpec.getLeftInputOpSpec().equals(prevOperatorSpec)) { // we got here from the left side of the join
      return new PartialJoinOperatorImpl(joinOpSpec, /* isLeftSide */ true,
          partialJoinFunctions.getKey(), partialJoinFunctions.getValue(), config, context, clock);
    } else { // we got here from the right side of the join
      return new PartialJoinOperatorImpl(joinOpSpec, /* isLeftSide */ false,
          partialJoinFunctions.getValue(), partialJoinFunctions.getKey(), config, context, clock);
    }
  }

  private KV<PartialJoinFunction, PartialJoinFunction> getOrCreatePartialJoinFunctions(JoinOperatorSpec joinOpSpec) {
    return joinFunctions.computeIfAbsent(joinOpSpec.getOpId(),
        joinOpId -> KV.of(createLeftJoinFn(joinOpSpec), createRightJoinFn(joinOpSpec)));
  }

  private PartialJoinFunction<Object, Object, Object, Object> createLeftJoinFn(JoinOperatorSpec joinOpSpec) {
    return new PartialJoinFunction<Object, Object, Object, Object>() {
      private final JoinFunction joinFn = joinOpSpec.getJoinFn();
      private KeyValueStore<Object, TimestampedValue<Object>> leftStreamState;

      @Override
      public Object apply(Object m, Object om) {
        return joinFn.apply(m, om);
      }

      @Override
      public Object getKey(Object message) {
        return joinFn.getFirstKey(message);
      }

      @Override
      public KeyValueStore<Object, TimestampedValue<Object>> getState() {
        return leftStreamState;
      }

      @Override
      public void init(Config config, TaskContext context) {
        String leftStoreName = joinOpSpec.getLeftOpId();
        leftStreamState = (KeyValueStore<Object, TimestampedValue<Object>>) context.getStore(leftStoreName);

        // user-defined joinFn should only be initialized once, so we do it only in left partial join function.
        joinFn.init(config, context);
      }

      @Override
      public void close() {
        // joinFn#close() must only be called once, so we do it it only in left partial join function.
        joinFn.close();
      }
    };
  }

  private PartialJoinFunction<Object, Object, Object, Object> createRightJoinFn(JoinOperatorSpec joinOpSpec) {
    return new PartialJoinFunction<Object, Object, Object, Object>() {
      private final JoinFunction joinFn = joinOpSpec.getJoinFn();
      private KeyValueStore<Object, TimestampedValue<Object>> rightStreamState;

      @Override
      public Object apply(Object m, Object om) {
        return joinFn.apply(om, m);
      }

      @Override
      public Object getKey(Object message) {
        return joinFn.getSecondKey(message);
      }

      @Override
      public void init(Config config, TaskContext context) {
        String rightStoreName = joinOpSpec.getRightOpId();
        rightStreamState = (KeyValueStore<Object, TimestampedValue<Object>>) context.getStore(rightStoreName);

        // user-defined joinFn should only be initialized once,
        // so we do it only in left partial join function and not here again.
      }

      @Override
      public KeyValueStore<Object, TimestampedValue<Object>> getState() {
        return rightStreamState;
      }
    };
  }

  private boolean hasIntermediateStreams(StreamGraphImpl streamGraph) {
    return !Collections.disjoint(streamGraph.getInputOperators().keySet(), streamGraph.getOutputStreams().keySet());
  }

  /**
   * calculate the task count that produces to each intermediate streams
   * @param streamToConsumerTasks input streams to task mapping
   * @param intermediateToInputStreams intermediate stream to input streams mapping
   * @return mapping from intermediate stream to task count
   */
  static Map<SystemStream, Integer> getProducerTaskCountForIntermediateStreams(
      Multimap<SystemStream, String> streamToConsumerTasks,
      Multimap<SystemStream, SystemStream> intermediateToInputStreams) {
    Map<SystemStream, Integer> result = new HashMap<>();
    intermediateToInputStreams.asMap().entrySet().forEach(entry -> {
        result.put(entry.getKey(),
            entry.getValue().stream()
                .flatMap(systemStream -> streamToConsumerTasks.get(systemStream).stream())
                .collect(Collectors.toSet()).size());
      });
    return result;
  }

  /**
   * calculate the mapping from input streams to consumer tasks
   * @param jobModel JobModel object
   * @return mapping from input stream to tasks
   */
  static Multimap<SystemStream, String> getStreamToConsumerTasks(JobModel jobModel) {
    Multimap<SystemStream, String> streamToConsumerTasks = HashMultimap.create();
    jobModel.getContainers().values().forEach(containerModel -> {
        containerModel.getTasks().values().forEach(taskModel -> {
            taskModel.getSystemStreamPartitions().forEach(ssp -> {
                streamToConsumerTasks.put(ssp.getSystemStream(), taskModel.getTaskName().getTaskName());
              });
          });
      });
    return streamToConsumerTasks;
  }

  /**
   * calculate the mapping from output streams to input streams
   * @param streamGraph the user {@link StreamGraphImpl} instance
   * @return mapping from output streams to input streams
   */
  static Multimap<SystemStream, SystemStream> getIntermediateToInputStreamsMap(StreamGraphImpl streamGraph) {
    Multimap<SystemStream, SystemStream> outputToInputStreams = HashMultimap.create();
    streamGraph.getInputOperators().entrySet().stream()
        .forEach(
            entry -> computeOutputToInput(entry.getKey().toSystemStream(), entry.getValue(), outputToInputStreams));
    return outputToInputStreams;
  }

  private static void computeOutputToInput(SystemStream input, OperatorSpec opSpec,
      Multimap<SystemStream, SystemStream> outputToInputStreams) {
    if (opSpec instanceof PartitionByOperatorSpec) {
      PartitionByOperatorSpec spec = (PartitionByOperatorSpec) opSpec;
      outputToInputStreams.put(spec.getOutputStream().getStreamSpec().toSystemStream(), input);
    } else {
      Collection<OperatorSpec> nextOperators = opSpec.getRegisteredOperatorSpecs();
      nextOperators.forEach(spec -> computeOutputToInput(input, spec, outputToInputStreams));
    }
  }
}
