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
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.task.TaskContext;


/**
 * The spec for a sink operator that accepts user-defined logic to output a {@link MessageStreamImpl} to an external
 * system. This is a terminal operator and does allows further operator chaining.
 *
 * @param <M>  the type of input message
 */
public class SinkOperatorSpec<M> implements OperatorSpec {

  /**
   * {@link OpCode} for this {@link SinkOperatorSpec}
   */
  private final OperatorSpec.OpCode opCode;

  /**
   * The unique ID for this operator.
   */
  private final int opId;

  /**
   * The user-defined sink function
   */
  private final SinkFunction<M> sinkFn;

  /**
   * Potential output stream defined by the {@link SinkFunction}
   */
  private final OutputStream<M> outStream;

  /**
   * Default constructor for a {@link SinkOperatorSpec} w/o an output stream. (e.g. output is sent to remote database)
   *
   * @param sinkFn  a user defined {@link SinkFunction} that will be called with the output message,
   *                the output {@link org.apache.samza.task.MessageCollector} and the
   *                {@link org.apache.samza.task.TaskCoordinator}.
   * @param opCode  the specific {@link OpCode} for this {@link SinkOperatorSpec}. It could be {@link OpCode#SINK}, {@link OpCode#SEND_TO},
   *                or {@link OpCode#PARTITION_BY}
   * @param opId  the unique id of this {@link SinkOperatorSpec} in the {@link org.apache.samza.operators.StreamGraph}
   */
  SinkOperatorSpec(SinkFunction<M> sinkFn, OperatorSpec.OpCode opCode, int opId) {
    this(sinkFn, opCode, opId, null);
  }

  /**
   * Default constructor for a {@link SinkOperatorSpec} that sends the output to an {@link OutputStream}
   *
   * @param sinkFn  a user defined {@link SinkFunction} that will be called with the output message,
   *                the output {@link org.apache.samza.task.MessageCollector} and the
   *                {@link org.apache.samza.task.TaskCoordinator}.
   * @param opCode  the specific {@link OpCode} for this {@link SinkOperatorSpec}. It could be {@link OpCode#SINK}, {@link OpCode#SEND_TO},
   *                or {@link OpCode#PARTITION_BY}
   * @param opId  the unique id of this {@link SinkOperatorSpec} in the {@link org.apache.samza.operators.StreamGraph}
   * @param opId  the {@link OutputStream} for this {@link SinkOperatorSpec}
   */
  SinkOperatorSpec(SinkFunction<M> sinkFn, OperatorSpec.OpCode opCode, int opId, OutputStream<M> outStream) {
    this.sinkFn = sinkFn;
    this.opCode = opCode;
    this.opId = opId;
    this.outStream = outStream;
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

  public OperatorSpec.OpCode getOpCode() {
    return this.opCode;
  }

  public int getOpId() {
    return this.opId;
  }

  public OutputStream<M> getOutStream() {
    return this.outStream;
  }

  @Override public void init(Config config, TaskContext context) {
    this.sinkFn.init(config, context);
  }
}
