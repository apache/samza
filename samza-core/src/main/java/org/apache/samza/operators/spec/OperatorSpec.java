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

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;

/**
 * A stream operator specification that holds all the information required to transform
 * the input {@link org.apache.samza.operators.MessageStreamImpl} and produce the output
 * {@link org.apache.samza.operators.MessageStreamImpl}.
 *
 * @param <M>  the type of input message to the operator
 * @param <OM>  the type of output message from the operator
 */
@InterfaceStability.Unstable
public abstract class OperatorSpec<M, OM> implements Serializable {

  public enum OpCode {
    INPUT,
    MAP,
    FLAT_MAP,
    FILTER,
    SINK,
    SEND_TO,
    JOIN,
    WINDOW,
    MERGE,
    PARTITION_BY,
    OUTPUT,
    BROADCAST
  }

  private final String opId;
  private final OpCode opCode;
  private StackTraceElement[] creationStackTrace;

  /**
   * The set of operators that consume the messages produced from this operator.
   * <p>
   * We use a LinkedHashSet since we need deterministic ordering in initializing/closing operators.
   *
   * Note: this is also made transient since the deserialized copies of {@link OperatorSpec} objects are made externally.
   */
  private transient final Set<OperatorSpec<OM, ?>> nextOperatorSpecs = new LinkedHashSet<>();

  @VisibleForTesting
  final boolean isClone(OperatorSpec other) {
    return this != other && this.getClass().isAssignableFrom(other.getClass())
        && this.opCode.equals(other.opCode) && this.opId.equals(other.opId);
  }

  public OperatorSpec(OpCode opCode, String opId) {
    this.opCode = opCode;
    this.opId = opId;
    this.creationStackTrace = Thread.currentThread().getStackTrace();
  }

  /**
   * Register the next operator spec in the chain that this operator should propagate its output to.
   * @param nextOperatorSpec  the next operator in the chain.
   */
  public void registerNextOperatorSpec(OperatorSpec<OM, ?> nextOperatorSpec) {
    nextOperatorSpecs.add(nextOperatorSpec);
  }

  /**
   * Get the collection of chained {@link OperatorSpec}s that are consuming the output of this node
   *
   * @return the collection of chained {@link OperatorSpec}s
   */
  public Collection<OperatorSpec<OM, ?>> getRegisteredOperatorSpecs() {
    return nextOperatorSpecs;
  }

  public static byte[] toByte(OperatorSpec opSpec) throws IOException {
    final ByteArrayOutputStream serializedBytes = new ByteArrayOutputStream();
    final ObjectOutputStream outputStream = new ObjectOutputStream(serializedBytes);
    outputStream.writeObject(opSpec);
    outputStream.close();
    return serializedBytes.toByteArray();
  }

  public static OperatorSpec fromByte(byte[] serializedBytes) throws IOException, ClassNotFoundException {
    final ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(serializedBytes));
    Object object = inputStream.readObject();
    inputStream.close();
    return (OperatorSpec) object;
  }

  /**
   * Get the {@link OpCode} for this operator.
   * @return  the {@link OpCode} for this operator
   */
  public final OpCode getOpCode() {
    return this.opCode;
  }

  /**
   * Get the unique ID of this operator in the {@link org.apache.samza.operators.StreamGraph}.
   * @return  the unique operator ID
   */
  public final String getOpId() {
    return this.opId;
  }

  /**
   * Get the user source code location that created the operator.
   * @return  source code location for the operator
   */
  public final String getSourceLocation() {
    // The stack trace for most operators looks like:
    // [0] Thread.getStackTrace()
    // [1] OperatorSpec.init<>()
    // [2] SomeOperatorSpec.<init>()
    // [3] OperatorSpecs.createSomeOperatorSpec()
    // [4] MessageStreamImpl.someOperator()
    // [5] User/MessageStreamImpl code that calls [4]
    // We are interested in the first call below this that originates from user code
    StackTraceElement element = this.creationStackTrace[5];

    /**
     * Sometimes [5] above is a call from MessageStream/MessageStreamImpl itself (e.g. for
     * {@link org.apache.samza.operators.MessageStream#mergeAll(Collection)} or
     * {@link MessageStreamImpl#partitionBy(Function, Function)}).
     * If that's the case, find the first call from a class other than these.
     */
    for (int i = 5; i < creationStackTrace.length; i++) {
      if (!creationStackTrace[i].getClassName().equals(MessageStreamImpl.class.getName())
          && !creationStackTrace[i].getClassName().equals(MessageStream.class.getName())) {
        element = creationStackTrace[i];
        break;
      }
    }
    return String.format("%s:%s", element.getFileName(), element.getLineNumber());
  }

  abstract public WatermarkFunction getWatermarkFn();

  abstract public TimerFunction getTimerFn();
}
