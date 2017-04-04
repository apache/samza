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
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.impl.OperatorImplGraph;
import org.apache.samza.operators.stream.InputStreamInternal;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;

import java.util.HashMap;
import java.util.Map;


/**
 * A {@link StreamTask} implementation that brings all the operator API implementation components together and
 * feeds the input messages into the user-defined transformation chains in {@link StreamApplication}.
 */
public final class StreamOperatorTask implements StreamTask, InitableTask, WindowableTask, ClosableTask {

  private final StreamApplication streamApplication;
  private final ApplicationRunner runner;
  private final Clock clock;

  private OperatorImplGraph operatorImplGraph;
  private ContextManager contextManager;
  private Map<SystemStream, InputStreamInternal> inputSystemStreamToInputStream;

  /**
   * Constructs an adaptor task to run the user-implemented {@link StreamApplication}.
   * @param streamApplication the user-implemented {@link StreamApplication} that creates the logical DAG
   * @param runner the {@link ApplicationRunner} to get the mapping between logical and physical streams
   * @param clock the {@link Clock} to use for time-keeping
   */
  public StreamOperatorTask(StreamApplication streamApplication, ApplicationRunner runner, Clock clock) {
    this.streamApplication = streamApplication;
    this.runner = runner;
    this.clock = clock;
  }

  public StreamOperatorTask(StreamApplication application, ApplicationRunner runner) {
    this(application, runner, SystemClock.instance());
  }

  /**
   * Initializes this task during startup.
   * <p>
   * Implementation: Initializes the user-implemented {@link StreamApplication}. The {@link StreamApplication} sets
   * the input and output streams and the task-wide context manager using the {@link StreamGraphImpl} APIs,
   * and the logical transforms using the {@link org.apache.samza.operators.MessageStream} APIs.
   *<p>
   * It then uses the {@link StreamGraphImpl} to create the {@link OperatorImplGraph} corresponding to the logical
   * DAG. It also saves the mapping between input {@link SystemStream}s and their corresponding
   * {@link InputStreamInternal}s for delivering incoming messages to the appropriate sub-DAG.
   *
   * @param config allows accessing of fields in the configuration files that this StreamTask is specified in
   * @param context allows initializing and accessing contextual data of this StreamTask
   * @throws Exception in case of initialization errors
   */
  @Override
  public final void init(Config config, TaskContext context) throws Exception {
    StreamGraphImpl streamGraph = new StreamGraphImpl(runner, config);
    // initialize the user-implemented stream application.
    this.streamApplication.init(streamGraph, config);

    // get the user-implemented context manager and initialize the task-specific context.
    this.contextManager = streamGraph.getContextManager();
    TaskContext initializedTaskContext = this.contextManager.initTaskContext(config, context);

    // create the operator impl DAG corresponding to the logical operator spec DAG
    OperatorImplGraph operatorImplGraph = new OperatorImplGraph(clock);
    operatorImplGraph.init(streamGraph, config, initializedTaskContext);
    this.operatorImplGraph = operatorImplGraph;

    // TODO: SAMZA-1118 - Remove mapping after SystemConsumer starts returning logical streamId with incoming messages
    inputSystemStreamToInputStream = new HashMap<>();
    streamGraph.getInputStreams().forEach((streamSpec, inputStream)-> {
        SystemStream systemStream = new SystemStream(streamSpec.getSystemName(), streamSpec.getPhysicalName());
        inputSystemStreamToInputStream.put(systemStream, inputStream);
      });
  }

  /**
   * Passes the incoming message envelopes along to the {@link org.apache.samza.operators.impl.RootOperatorImpl} node
   * for the input {@link SystemStream}.
   * <p>
   * From then on, each {@link org.apache.samza.operators.impl.OperatorImpl} propagates its transformed output to
   * its chained {@link org.apache.samza.operators.impl.OperatorImpl}s itself.
   *
   * @param ime incoming message envelope to process
   * @param collector the collector to send messages with
   * @param coordinator the coordinator to request commits or shutdown
   */
  @Override
  public final void process(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator) {
    SystemStream systemStream = ime.getSystemStreamPartition().getSystemStream();
    InputStreamInternal inputStream = inputSystemStreamToInputStream.get(systemStream);
    // TODO: SAMZA-1148 - Cast to appropriate input (key, msg) types based on the serde before applying the msgBuilder.
    operatorImplGraph.getRootOperator(systemStream)
        .onNext(inputStream.getMsgBuilder().apply(ime.getKey(), ime.getMessage()), collector, coordinator);
  }

  @Override
  public final void window(MessageCollector collector, TaskCoordinator coordinator)  {
    operatorImplGraph.getAllRootOperators()
        .forEach(rootOperator -> rootOperator.onTick(collector, coordinator));
  }

  @Override
  public void close() throws Exception {
    this.contextManager.finalizeTaskContext();
  }
}
