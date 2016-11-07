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
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreams;
import org.apache.samza.operators.MessageStreams.SystemMessageStream;
import org.apache.samza.operators.data.IncomingSystemMessage;
import org.apache.samza.operators.impl.ChainedOperators;
import org.apache.samza.operators.task.StreamOperatorTask;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import java.util.HashMap;
import java.util.Map;


/**
 * An adaptor task class that invoke the user-implemented (@link StreamOperatorTask}s via {@link MessageStream} programming APIs
 *
 */
public final class StreamOperatorAdaptorTask implements StreamTask, InitableTask, WindowableTask {
  /**
   * A map with entries mapping {@link SystemStreamPartition} to {@link ChainedOperators} that takes the {@link SystemStreamPartition}
   * as the input stream
   */
  private final Map<SystemStreamPartition, ChainedOperators> operatorChains = new HashMap<>();

  /**
   * Wrapped {@link StreamOperatorTask} class
   */
  private final StreamOperatorTask  userTask;

  /**
   * Constructor that wraps the user-defined {@link StreamOperatorTask}
   *
   * @param userTask  the user-defined {@link StreamOperatorTask}
   */
  public StreamOperatorAdaptorTask(StreamOperatorTask userTask) {
    this.userTask = userTask;
  }

  @Override
  public final void init(Config config, TaskContext context) throws Exception {
    if (this.userTask instanceof InitableTask) {
      ((InitableTask) this.userTask).init(config, context);
    }
    Map<SystemStreamPartition, SystemMessageStream> sources = new HashMap<>();
    context.getSystemStreamPartitions().forEach(ssp -> {
      SystemMessageStream ds = MessageStreams.input(ssp);
      sources.put(ssp, ds);
    });
    this.userTask.initOperators(sources.values());
    sources.forEach((ssp, ds) -> operatorChains.put(ssp, ChainedOperators.create(ds, context)));
  }

  @Override
  public final void process(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator) {
    this.operatorChains.get(ime.getSystemStreamPartition()).onNext(new IncomingSystemMessage(ime), collector, coordinator);
  }

  @Override
  public final void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception{
    this.operatorChains.forEach((ssp, chain) -> chain.onTimer(collector, coordinator));
    if (this.userTask instanceof WindowableTask) {
      ((WindowableTask) this.userTask).window(collector, coordinator);
    }
  }

}
