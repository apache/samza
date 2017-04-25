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

import java.util.Map;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.task.TaskContext;


/**
 * A stream operator specification that holds all the information required to transform 
 * the input {@link MessageStreamImpl} and produce the output {@link MessageStreamImpl}.
 *
 * @param <OM>  the type of output message from the operator
 */
@InterfaceStability.Unstable
public interface OperatorSpec<OM> {

  enum OpCode {
    MAP,
    FLAT_MAP,
    FILTER,
    SINK,
    SEND_TO,
    JOIN,
    WINDOW,
    MERGE,
    PARTITION_BY
  }

  /**
   * Get the next {@link MessageStreamImpl} that receives the transformed messages produced by this operator.
   * @return  the next {@link MessageStreamImpl}
   */
  MessageStreamImpl<OM> getNextStream();

  /**
   * Get the {@link OpCode} for this operator.
   * @return  the {@link OpCode} for this operator
   */
  OpCode getOpCode();

  /**
   * Get the unique ID of this operator in the {@link org.apache.samza.operators.StreamGraph}.
   * @return  the unique operator ID
   */
  int getOpId();

  /**
   * Return a map object for JSON representation of the operator
   * @return a map of JSON POJO objects
   */
  Map<String, Object> toJsonMap();

  /**
   * Return the user source code location that creates the operator
   * @return source location
   */
  StackTraceElement getSourceLocation();

  /**
   * Init method to initialize the context for this {@link OperatorSpec}. The default implementation is NO-OP.
   *
   * @param config  the {@link Config} object for this task
   * @param context  the {@link TaskContext} object for this task
   */
  default void init(Config config, TaskContext context) { }
}
