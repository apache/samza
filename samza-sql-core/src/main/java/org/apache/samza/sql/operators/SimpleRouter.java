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

package org.apache.samza.sql.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.RouterMessageCollector;


/**
 * Example implementation of {@link org.apache.samza.sql.api.operators.OperatorRouter}
 *
 */
public final class SimpleRouter implements OperatorRouter {
  /**
   * List of operators added to the {@link org.apache.samza.sql.api.operators.OperatorRouter}
   */
  private List<SimpleOperator> operators = new ArrayList<SimpleOperator>();

  @SuppressWarnings("rawtypes")
  /**
   * Map of {@link org.apache.samza.sql.api.data.EntityName} to the list of operators associated with it
   */
  private Map<EntityName, List> nextOps = new HashMap<EntityName, List>();

  /**
   * Set of {@link org.apache.samza.sql.api.data.EntityName} as inputs to this {@code SimpleRouter}
   */
  private Set<EntityName> inputEntities = new HashSet<EntityName>();

  /**
   * Set of entities that are not input entities to this {@code SimpleRouter}
   */
  private Set<EntityName> outputEntities = new HashSet<EntityName>();

  @SuppressWarnings("unchecked")
  private void addOperator(EntityName input, SimpleOperator nextOp) {
    if (nextOps.get(input) == null) {
      nextOps.put(input, new ArrayList<Operator>());
    }
    nextOps.get(input).add(nextOp);
    operators.add(nextOp);
    // get the operator spec
    for (EntityName output : nextOp.getSpec().getOutputNames()) {
      if (inputEntities.contains(output)) {
        inputEntities.remove(output);
      }
      outputEntities.add(output);
    }
    if (!outputEntities.contains(input)) {
      inputEntities.add(input);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<SimpleOperator> getNextOperators(EntityName entity) {
    return nextOps.get(entity);
  }

  @Override
  public void addOperator(SimpleOperator nextOp) {
    List<EntityName> inputs = nextOp.getSpec().getInputNames();
    for (EntityName input : inputs) {
      addOperator(input, nextOp);
    }
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    for (SimpleOperator op : this.operators) {
      op.init(config, context);
    }
  }

  @Override
  public void process(Tuple ituple, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    MessageCollector opCollector = new RouterMessageCollector(collector, coordinator, this);
    for (Iterator<SimpleOperator> iter = this.getNextOperators(ituple.getEntityName()).iterator(); iter.hasNext();) {
      iter.next().process(ituple, opCollector, coordinator);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void process(Relation deltaRelation, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    MessageCollector opCollector = new RouterMessageCollector(collector, coordinator, this);
    for (Iterator<SimpleOperator> iter = this.getNextOperators(deltaRelation.getName()).iterator(); iter.hasNext();) {
      iter.next().process(deltaRelation, opCollector, coordinator);
    }
  }

  @Override
  public void refresh(long nanoSec, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    MessageCollector opCollector = new RouterMessageCollector(collector, coordinator, this);
    for (EntityName entity : inputEntities) {
      for (Iterator<SimpleOperator> iter = this.getNextOperators(entity).iterator(); iter.hasNext();) {
        iter.next().refresh(nanoSec, opCollector, coordinator);
      }
    }
  }

  @Override
  public Iterator<SimpleOperator> iterator() {
    return this.operators.iterator();
  }

}
