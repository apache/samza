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
package org.apache.samza.task;

import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.data.InputMessageEnvelope;
import org.apache.samza.operators.impl.OperatorGraph;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;

import java.util.HashMap;
import java.util.Map;


/**
 * Execution of the logic sub-DAG
 *
 *
 * An {@link StreamTask} implementation that receives {@link InputMessageEnvelope}s and propagates them
 * through the user's stream transformations defined in {@link StreamGraphImpl} using the
 * {@link org.apache.samza.operators.MessageStream} APIs.
 * <p>
 * This class brings all the operator API implementation components together and feeds the
 * {@link InputMessageEnvelope}s into the transformation chains.
 * <p>
 * It accepts an instance of the user implemented factory {@link StreamGraphBuilder} as input parameter of the constructor.
 * When its own {@link #init(Config, TaskContext)} method is called during startup, it instantiate a user-defined {@link StreamGraphImpl}
 * from the {@link StreamGraphBuilder}, calls {@link StreamGraphImpl#getContextManager()} to initialize the task-wide context
 * for the graph, and creates a {@link MessageStreamImpl} corresponding to each of its input
 * {@link org.apache.samza.system.SystemStreamPartition}s. Each input {@link MessageStreamImpl}
 * will be corresponding to either an input stream or intermediate stream in {@link StreamGraphImpl}.
 * <p>
 * Then, this task calls {@link org.apache.samza.operators.impl.OperatorGraph#init(Map, Config, TaskContext)} for each of the input
 * {@link MessageStreamImpl}. This instantiates the {@link org.apache.samza.operators.impl.OperatorImpl} DAG
 * corresponding to the aforementioned {@link org.apache.samza.operators.spec.OperatorSpec} DAG and returns the
 * root node of the DAG, which this class saves.
 * <p>
 * Now that it has the root for the DAG corresponding to each {@link org.apache.samza.system.SystemStreamPartition}, it
 * can pass the message envelopes received in {@link StreamTask#process(IncomingMessageEnvelope, MessageCollector, TaskCoordinator)}
 * along to the appropriate root nodes. From then on, each {@link org.apache.samza.operators.impl.OperatorImpl} propagates
 * its transformed output to the next set of {@link org.apache.samza.operators.impl.OperatorImpl}s.
 */
public final class StreamOperatorTask implements StreamTask, InitableTask, WindowableTask, ClosableTask {

  /**
   * A mapping from each {@link SystemStream} to the root node of its operator chain DAG.
   */
  private final OperatorGraph operatorGraph = new OperatorGraph();

  private final StreamGraphBuilder graphBuilder;

  private ContextManager contextManager;

  public StreamOperatorTask(StreamGraphBuilder graphBuilder) {
    this.graphBuilder = graphBuilder;
  }

  @Override
  public final void init(Config config, TaskContext context) throws Exception {
    // for now, we need to create the execution env again
    // in the future if we decide to serialize the dag, this can be clean up
    ExecutionEnvironment executionEnvironment = ExecutionEnvironment.fromConfig(config);
    // create the MessageStreamsImpl object and initialize app-specific logic DAG within the task
    StreamGraphImpl streams = new StreamGraphImpl(executionEnvironment);
    this.graphBuilder.init(streams, config);
    // get the context manager of the {@link StreamGraph} and initialize the task-specific context
    this.contextManager = streams.getContextManager();

    Map<SystemStream, MessageStreamImpl> inputBySystemStream = new HashMap<>();
    context.getSystemStreamPartitions().forEach(ssp -> {
        if (!inputBySystemStream.containsKey(ssp.getSystemStream())) {
          // create mapping from the physical input {@link SystemStream} to the logic {@link MessageStream}
          inputBySystemStream.putIfAbsent(ssp.getSystemStream(), streams.getInputStream(ssp.getSystemStream()));
        }
      });
    operatorGraph.init(inputBySystemStream, config, this.contextManager.initTaskContext(config, context));
  }

  @Override
  public final void process(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator) {
    this.operatorGraph.get(ime.getSystemStreamPartition().getSystemStream())
        .onNext(new InputMessageEnvelope(ime), collector, coordinator);
  }

  @Override
  public final void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO: invoke timer based triggers
  }

  @Override
  public void close() throws Exception {
    this.contextManager.finalizeTaskContext();
  }
}
