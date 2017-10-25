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
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.MessageType;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.impl.InputOperatorImpl;
import org.apache.samza.operators.impl.OperatorImplGraph;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamTask} implementation that brings all the operator API implementation components together and
 * feeds the input messages into the user-defined transformation chains in {@link StreamApplication}.
 */
public class StreamOperatorTask implements StreamTask, InitableTask, WindowableTask, ClosableTask {
  private static final Logger LOG = LoggerFactory.getLogger(StreamOperatorTask.class);

  private final StreamApplication streamApplication;
  private final ApplicationRunner runner;
  private final Clock clock;

  private OperatorImplGraph operatorImplGraph;
  private ContextManager contextManager;

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
   * and the logical transforms using the {@link org.apache.samza.operators.MessageStream} APIs. It then uses
   * the {@link StreamGraphImpl} to create the {@link OperatorImplGraph} corresponding to the logical DAG.
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

    // get the user-implemented context manager and initialize it
    this.contextManager = streamGraph.getContextManager();
    if (this.contextManager != null) {
      this.contextManager.init(config, context);
    }

    // create the operator impl DAG corresponding to the logical operator spec DAG
    this.operatorImplGraph = new OperatorImplGraph(streamGraph, config, context, clock);
  }

  /**
   * Passes the incoming message envelopes along to the {@link InputOperatorImpl} node
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
    InputOperatorImpl inputOpImpl = operatorImplGraph.getInputOperator(systemStream);
    if (inputOpImpl != null) {
      switch (MessageType.of(ime.getMessage())) {
        case USER_MESSAGE:
          inputOpImpl.onMessage(KV.of(ime.getKey(), ime.getMessage()), collector, coordinator);
          break;

        case END_OF_STREAM:
          EndOfStreamMessage eosMessage = (EndOfStreamMessage) ime.getMessage();
          inputOpImpl.aggregateEndOfStream(eosMessage, ime.getSystemStreamPartition(), collector, coordinator);
          break;

        case WATERMARK:
          WatermarkMessage watermarkMessage = (WatermarkMessage) ime.getMessage();
          inputOpImpl.aggregateWatermark(watermarkMessage, ime.getSystemStreamPartition(), collector, coordinator);
          break;
      }
    }
  }

  @Override
  public final void window(MessageCollector collector, TaskCoordinator coordinator)  {
    operatorImplGraph.getAllInputOperators()
        .forEach(inputOperator -> inputOperator.onTimer(collector, coordinator));
  }

  @Override
  public void close() throws Exception {
    if (this.contextManager != null) {
      this.contextManager.close();
    }
    operatorImplGraph.close();
  }

  /* package private for testing */
  OperatorImplGraph getOperatorImplGraph() {
    return this.operatorImplGraph;
  }
}
