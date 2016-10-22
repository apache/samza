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
package org.apache.samza.operators.impl;

import org.apache.commons.collections.keyvalue.AbstractMapEntry;
import org.apache.samza.operators.api.WindowState;
import org.apache.samza.operators.api.data.Message;
import org.apache.samza.operators.api.internal.Operators.*;
import org.apache.samza.operators.api.internal.WindowOutput;
import org.apache.samza.operators.impl.join.PartialJoinOpImpl;
import org.apache.samza.operators.impl.window.SessionWindowImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;


/**
 * The factory class that instantiates all implementation of {@link OperatorImpl} classes.
 */
public class OperatorFactory {

  /**
   * the static operatorMap that includes all operator implementation instances
   */
  private static final Map<Operator, OperatorImpl<? extends Message, ? extends Message>> operatorMap = new ConcurrentHashMap<>();

  /**
   * The method to actually create the implementation instances of operators
   *
   * @param operator  the immutable definition of {@link Operator}
   * @param <M>  type of input {@link Message}
   * @param <RM>  type of output {@link Message}
   * @return  the implementation object of {@link OperatorImpl}
   */
  private static <M extends Message, RM extends Message> OperatorImpl<M, ? extends Message> createOperator(Operator<RM> operator) {
    if (operator instanceof StreamOperator) {
      return new SimpleOperatorImpl<>((StreamOperator<M, RM>) operator);
    } else if (operator instanceof SinkOperator) {
      return new SinkOperatorImpl<>((SinkOperator<M>) operator);
    } else if (operator instanceof WindowOperator) {
      return new SessionWindowImpl<>((WindowOperator<M, ?, ? extends WindowState, ? extends WindowOutput>) operator);
    } else if (operator instanceof PartialJoinOperator) {
      return new PartialJoinOpImpl<>((PartialJoinOperator) operator);
    }
    throw new IllegalArgumentException(
        String.format("The type of operator is not supported. Operator class name: %s", operator.getClass().getName()));
  }

  /**
   * The method to get the unique implementation instance of {@link Operator}
   *
   * @param operator  the {@link Operator} to instantiate
   * @param <M>  type of input {@link Message}
   * @param <RM>  type of output {@link Message}
   * @return  A pair of entry that include the unique implementation instance to the {@code operator} and a boolean value indicating whether
   *          the operator instance has already been created or not. True means the operator instance has already created, false means the operator
   *          was not created.
   */
  public static <M extends Message, RM extends Message> Entry<OperatorImpl<M, ? extends Message>, Boolean> getOperator(Operator<RM> operator) {
    if (!operatorMap.containsKey(operator)) {
      OperatorImpl<M, ? extends Message> operatorImpl = OperatorFactory.createOperator(operator);
      if( operatorMap.putIfAbsent(operator, operatorImpl) == null ) {
        return new AbstractMapEntry(operatorImpl, false) {};
      }
    }
    return new AbstractMapEntry((OperatorImpl<M, ? extends Message>) operatorMap.get(operator), true) {};
  }

}
