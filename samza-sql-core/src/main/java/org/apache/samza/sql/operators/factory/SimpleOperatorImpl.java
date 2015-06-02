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

package org.apache.samza.sql.operators.factory;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;


/**
 * An abstract class that encapsulate the basic information and methods that all operator classes should implement.
 * It implements the interface {@link org.apache.samza.sql.api.operators.SimpleOperator}
 *
 */
public abstract class SimpleOperatorImpl implements SimpleOperator {
  /**
   * The specification of this operator
   */
  private final OperatorSpec spec;

  /**
   * The callback function
   */
  private final OperatorCallback callback;

  /**
   * Ctor of {@code SimpleOperatorImpl} class
   *
   * @param spec The specification of this operator
   */
  public SimpleOperatorImpl(OperatorSpec spec) {
    this(spec, new NoopOperatorCallback());
  }

  public SimpleOperatorImpl(OperatorSpec spec, OperatorCallback callback) {
    this.spec = spec;
    this.callback = callback;
  }

  @Override
  public OperatorSpec getSpec() {
    return this.spec;
  }

  /**
   * This method is made final s.t. the sequence of invocations between {@link org.apache.samza.sql.api.operators.OperatorCallback#beforeProcess(Relation, MessageCollector, TaskCoordinator)}
   * and real processing of the input relation is fixed.
   */
  @Override
  final public void process(Relation deltaRelation, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    Relation rel = this.callback.beforeProcess(deltaRelation, collector, coordinator);
    if (rel == null) {
      return;
    }
    this.realProcess(rel, getCollector(collector, coordinator), coordinator);
  }

  /**
   * This method is made final s.t. the sequence of invocations between {@link org.apache.samza.sql.api.operators.OperatorCallback#beforeProcess(Tuple, MessageCollector, TaskCoordinator)}
   * and real processing of the input tuple is fixed.
   */
  @Override
  final public void process(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    Tuple ituple = this.callback.beforeProcess(tuple, collector, coordinator);
    if (ituple == null) {
      return;
    }
    this.realProcess(ituple, getCollector(collector, coordinator), coordinator);
  }

  /**
   * This method is made final s.t. we enforce the invocation of {@code SimpleOperatorImpl#getCollector(MessageCollector, TaskCoordinator)} before doing anything futher
   */
  @Override
  final public void refresh(long timeNano, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    this.realRefresh(timeNano, getCollector(collector, coordinator), coordinator);
  }

  private SimpleMessageCollector getCollector(MessageCollector collector, TaskCoordinator coordinator) {
    if (!(collector instanceof SimpleMessageCollector)) {
      return new SimpleMessageCollector(collector, coordinator, this.callback);
    } else {
      ((SimpleMessageCollector) collector).switchOperatorCallback(this.callback);
      return (SimpleMessageCollector) collector;
    }
  }

  /**
   * Method to be overriden by each specific implementation class of operator to handle timeout event
   *
   * @param timeNano The time in nanosecond when the timeout event occurred
   * @param collector The {@link org.apache.samza.task.sql.SimpleMessageCollector} in the context
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in the context
   * @throws Exception Throws exception if failed to refresh the results
   */
  protected abstract void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception;

  /**
   * Method to be overriden by each specific implementation class of operator to perform relational logic operation on an input {@link org.apache.samza.sql.api.data.Relation}
   *
   * @param rel The input relation
   * @param collector The {@link org.apache.samza.task.sql.SimpleMessageCollector} in the context
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in the context
   * @throws Exception
   */
  protected abstract void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception;

  protected abstract void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception;

}
