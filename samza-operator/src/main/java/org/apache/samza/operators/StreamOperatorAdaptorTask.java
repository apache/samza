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

import org.apache.samza.config.Config;
import org.apache.samza.operators.data.IncomingSystemMessage;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.impl.OperatorImpl;
import org.apache.samza.operators.impl.OperatorImpls;
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
 * An {@link StreamTask} implementation that receives incoming system messages and propagates them through the
 * user's stream transformations defined in {@link StreamOperatorTask#transform(Map)} using the {@link MessageStream} APIs.
 * <p>
 * This class brings all the operator API implementation components together and feeds incoming messages into the
 * transformation chains.
 * <p>
 * It accepts an instance of the user implemented {@link StreamOperatorTask}. When its own {@link #init(Config, TaskContext)}
 * method is called during startup, it creates a {@link MessageStreamImpl} corresponding to each of its input
 * {@link SystemStreamPartition}s and then calls the user's {@link StreamOperatorTask#transform(Map)} method.
 * <p>
 * When users invoke the methods on the {@link MessageStream} API to describe their stream transformations in the
 * {@link StreamOperatorTask#transform(Map)} method, the underlying {@link MessageStreamImpl} creates the
 * corresponding {@link org.apache.samza.operators.spec.OperatorSpec} to record information about the desired
 * transformation, and returns the output {@link MessageStream} to allow further transform chaining.
 * <p>
 * Once the user's transformation DAGs have been described for all {@link MessageStream}s (i.e., when the
 * {@link StreamOperatorTask#transform(Map)} call returns), it calls
 * {@link OperatorImpls#createOperatorImpls(MessageStreamImpl, TaskContext)} for each of the input
 * {@link MessageStreamImpl}. This instantiates the {@link org.apache.samza.operators.impl.OperatorImpl} DAG
 * corresponding to the aforementioned {@link org.apache.samza.operators.spec.OperatorSpec} DAG and returns the
 * root node of the DAG, which this class saves.
 * <p>
 * Now that it has the root for the DAG corresponding to each {@link SystemStreamPartition}, it can pass the messages
 * received in {@link StreamTask#process(IncomingMessageEnvelope, MessageCollector, TaskCoordinator)} along to the
 * appropriate root nodes. From then on, each {@link org.apache.samza.operators.impl.OperatorImpl} propagates its
 * transformed output to the next set of {@link org.apache.samza.operators.impl.OperatorImpl}s.
 */
public final class StreamOperatorAdaptorTask implements StreamTask, InitableTask, WindowableTask {

  /**
   * A mapping from each {@link SystemStreamPartition} to the root node of its operator chain DAG.
   */
  private final Map<SystemStreamPartition, OperatorImpl<IncomingSystemMessage, ? extends Message>> operatorChains = new HashMap<>();

  private final StreamOperatorTask userTask;

  public StreamOperatorAdaptorTask(StreamOperatorTask userTask) {
    this.userTask = userTask;
  }

  @Override
  public final void init(Config config, TaskContext context) throws Exception {
    if (this.userTask instanceof InitableTask) {
      ((InitableTask) this.userTask).init(config, context);
    }
    Map<SystemStreamPartition, MessageStream<IncomingSystemMessage>> messageStreams = new HashMap<>();
    context.getSystemStreamPartitions().forEach(ssp -> messageStreams.put(ssp, new MessageStreamImpl<>()));
    this.userTask.transform(messageStreams);
    messageStreams.forEach((ssp, ms) ->
        operatorChains.put(ssp, OperatorImpls.createOperatorImpls((MessageStreamImpl<IncomingSystemMessage>) ms, context)));
  }

  @Override
  public final void process(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator) {
    this.operatorChains.get(ime.getSystemStreamPartition())
        .onNext(new IncomingSystemMessage(ime), collector, coordinator);
  }

  @Override
  public final void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (this.userTask instanceof WindowableTask) {
      ((WindowableTask) this.userTask).window(collector, coordinator);
    }
  }
}
