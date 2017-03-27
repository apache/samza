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
package org.apache.samza.operators.spec;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.stream.OutputStream;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * The spec for an operator that outputs a {@link MessageStreamImpl} to an external system.
 * This is a terminal operator and does not allow further operator chaining.
 *
 * @param <M>  the type of input message
 */
public class OutputOperatorSpec<M> implements OperatorSpec {

  private final SinkFunction<M> sinkFn;
  private final OperatorSpec.OpCode opCode;
  private final int opId;

  /**
   * Constructs an {@link OutputOperatorSpec} with a user defined {@link SinkFunction}.
   *
   * @param sinkFn  a user defined {@link SinkFunction} that will be called with the output message,
   *                the output {@link org.apache.samza.task.MessageCollector} and the
   *                {@link org.apache.samza.task.TaskCoordinator}.
   * @param opCode  the specific {@link OpCode} for this {@link OutputOperatorSpec}.
   *                It could be {@link OpCode#SINK}, {@link OpCode#SEND_TO}, or {@link OpCode#PARTITION_BY}.
   * @param opId  the unique ID of this {@link OperatorSpec} in the graph
   */
  OutputOperatorSpec(SinkFunction<M> sinkFn, OperatorSpec.OpCode opCode, int opId) {
    this.sinkFn = sinkFn;
    this.opCode = opCode;
    this.opId = opId;
  }

  /**
   * Constructs an {@link OutputOperatorSpec} to send messages to the provided {@code outStream}
   * @param output  the output {@link MessageStreamImpl} to send messages to
   * @param opCode the specific {@link OpCode} for this {@link OutputOperatorSpec}.
   *               It could be {@link OpCode#SINK}, {@link OpCode#SEND_TO}, or {@link OpCode#PARTITION_BY}
   * @param opId  the unique ID of this {@link OutputOperatorSpec} in the graph
   */
  OutputOperatorSpec(MessageStreamImpl<M> output, OperatorSpec.OpCode opCode, int opId) {
    this(createSinkFn(output), opCode, opId);
  }

  /**
   * This is a terminal operator and doesn't allow further operator chaining.
   * @return  null
   */
  @Override
  public MessageStreamImpl<M> getNextStream() {
    return null;
  }

  public SinkFunction<M> getSinkFn() {
    return this.sinkFn;
  }

  @Override
  public OperatorSpec.OpCode getOpCode() {
    return this.opCode;
  }

  @Override
  public int getOpId() {
    return this.opId;
  }

  @Override
  public void init(Config config, TaskContext context) {
    this.sinkFn.init(config, context);
  }

  /**
   * Creates a {@link SinkFunction} to send messages to the provided {@code output}.
   * @param output  the output {@link MessageStreamImpl} to send messages to
   * @param <M>  the type of input message
   * @return  a {@link SinkFunction} that sends messages to the provided {@code output}
   */
  private static <M> SinkFunction<M> createSinkFn(MessageStreamImpl<M> output) {
    if (!(output instanceof OutputStream)) {
      throw new SamzaException("MessageStream used for sending messages must be an output stream.");
    }

    OutputStream<?, ?, M> outputStream = (OutputStream<?, ?, M>) output;
    return (M message, MessageCollector mc, TaskCoordinator tc) -> {
      // TODO: SAMZA-1148 - need to find a way to directly pass in the serde class names
      SystemStream systemStream = new SystemStream(outputStream.getStreamSpec().getSystemName(),
          outputStream.getStreamSpec().getPhysicalName());
      Object key = outputStream.getKeyExtractor().apply(message);
      Object msg = outputStream.getMsgExtractor().apply(message);
      mc.send(new OutgoingMessageEnvelope(systemStream, key, msg));
    };
  }
}
