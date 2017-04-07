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

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.stream.OutputStreamInternal;
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
public class SinkOperatorSpec<M> implements OperatorSpec {

  private final SinkFunction<M> sinkFn;
  private OutputStreamInternal<?, ?, M> outputStream; // may be null
  private final OperatorSpec.OpCode opCode;
  private final int opId;

  /**
   * Constructs a {@link SinkOperatorSpec} with a user defined {@link SinkFunction}.
   *
   * @param sinkFn  a user defined {@link SinkFunction} that will be called with the output message,
   *                the output {@link org.apache.samza.task.MessageCollector} and the
   *                {@link org.apache.samza.task.TaskCoordinator}.
   * @param opCode  the specific {@link OpCode} for this {@link SinkOperatorSpec}.
   *                It could be {@link OpCode#SINK}, {@link OpCode#SEND_TO}, or {@link OpCode#PARTITION_BY}.
   * @param opId  the unique ID of this {@link OperatorSpec} in the graph
   */
  SinkOperatorSpec(SinkFunction<M> sinkFn, OperatorSpec.OpCode opCode, int opId) {
    this.sinkFn = sinkFn;
    this.opCode = opCode;
    this.opId = opId;
  }

  /**
   * Constructs a {@link SinkOperatorSpec} to send messages to the provided {@code outStream}
   * @param outputStream  the {@link OutputStreamInternal} to send messages to
   * @param opCode the specific {@link OpCode} for this {@link SinkOperatorSpec}.
   *               It could be {@link OpCode#SINK}, {@link OpCode#SEND_TO}, or {@link OpCode#PARTITION_BY}
   * @param opId  the unique ID of this {@link SinkOperatorSpec} in the graph
   */
  SinkOperatorSpec(OutputStreamInternal<?, ?, M> outputStream, OperatorSpec.OpCode opCode, int opId) {
    this(createSinkFn(outputStream), opCode, opId);
    this.outputStream = outputStream;
  }

  /**
   * This is a terminal operator and doesn't allow further operator chaining.
   * @return  null
   */
  @Override
  public MessageStreamImpl<M> getNextStream() {
    return null;
  }

  /**
   * The {@link OutputStreamInternal} that this operator is sending its output to.
   * @return the {@link OutputStreamInternal} for this operator if any, else null.
   */
  public OutputStreamInternal<?, ?, M> getOutputStream() {
    return this.outputStream;
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
   * @param outputStream  the {@link OutputStreamInternal} to send messages to
   * @param <M>  the type of input message
   * @return  a {@link SinkFunction} that sends messages to the provided {@code output}
   */
  private static <M> SinkFunction<M> createSinkFn(OutputStreamInternal<?, ?, M> outputStream) {
    return (M message, MessageCollector mc, TaskCoordinator tc) -> {
      // TODO: SAMZA-1148 - need to find a way to directly pass in the serde class names
      SystemStream systemStream = new SystemStream(outputStream.getStreamSpec().getSystemName(),
          outputStream.getStreamSpec().getPhysicalName());
      Object key = outputStream.getKeyExtractor().apply(message);
      Object msg = outputStream.getMsgExtractor().apply(message);
      System.out.println("inside 11key " + key.toString() + " 11msg " + msg.toString());
      mc.send(new OutgoingMessageEnvelope(systemStream, key, msg));
    };
  }
}
