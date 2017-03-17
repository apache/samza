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

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.impl.OperatorGraph;
import org.apache.samza.operators.stream.InputStreamImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Execution of the logic sub-DAG
 *
 *
 * An {@link StreamTask} implementation that receives input messages and propagates them
 * through the user's stream transformations defined in {@link StreamGraphImpl} using the
 * {@link org.apache.samza.operators.MessageStream} APIs.
 * <p>
 * This class brings all the operator API implementation components together and feeds the
 * input messages into the transformation chains.
 * <p>
 * It accepts an instance of the user implemented factory {@link StreamApplication} as input parameter of the constructor.
 * When its own {@link #init(Config, TaskContext)} method is called during startup, it instantiate a user-defined {@link StreamGraphImpl}
 * from the {@link StreamApplication}, calls {@link StreamGraphImpl#getContextManager()} to initialize the task-wide context
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
  private final OperatorGraph operatorGraph;
  private final StreamApplication streamApplication;
  private final ApplicationRunner runner;

  private ContextManager contextManager;
  private StreamGraphImpl streamGraph;
  private Set<SystemStreamPartition> systemStreamPartitions;

  public StreamOperatorTask(StreamApplication application, ApplicationRunner runner) {
    this(application, runner, SystemClock.instance());
  }

  // for testing.
  public StreamOperatorTask(StreamApplication streamApplication, ApplicationRunner runner, Clock clock) {
    this.streamApplication = streamApplication;
    this.operatorGraph = new OperatorGraph(clock);
    this.runner = runner;
  }

  @Override
  public final void init(Config config, TaskContext context) throws Exception {
    streamGraph = new StreamGraphImpl(this.runner, config);
    // create the MessageStreamsImpl object and initialize app-specific logic DAG within the task
    this.streamApplication.init(streamGraph, config);
    // get the context manager of the {@link StreamGraph} and initialize the task-specific context
    this.contextManager = streamGraph.getContextManager();
    this.systemStreamPartitions = context.getSystemStreamPartitions();

    Map<SystemStream, MessageStreamImpl> inputBySystemStream = new HashMap<>();
    systemStreamPartitions.forEach(ssp -> {
        if (!inputBySystemStream.containsKey(ssp.getSystemStream())) {
          // create mapping from the physical input {@link SystemStream} to the logic {@link MessageStream}
          inputBySystemStream.putIfAbsent(ssp.getSystemStream(), streamGraph.getInputStream(ssp.getSystemStream()));
        }
      });
    operatorGraph.init(inputBySystemStream, config, this.contextManager.initTaskContext(config, context));
  }

  @Override
  public final void process(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator) {
    InputStreamImpl inputStream =
        (InputStreamImpl) streamGraph.getInputStream(ime.getSystemStreamPartition().getSystemStream());
    // TODO: SAMZA-1148 - Cast to appropriate input (key, msg) types based on the serde before applying the msgBuilder.
    this.operatorGraph.get(ime.getSystemStreamPartition().getSystemStream())
        .onNext(inputStream.getMsgBuilder().apply(ime.getKey(), ime.getMessage()), collector, coordinator);
  }

  @Override
  public final void window(MessageCollector collector, TaskCoordinator coordinator)  {
    systemStreamPartitions.forEach(ssp -> {
        this.operatorGraph.get(ssp.getSystemStream())
          .onTick(collector, coordinator);
      });
  }

  @Override
  public void close() throws Exception {
    this.contextManager.finalizeTaskContext();
  }
}
