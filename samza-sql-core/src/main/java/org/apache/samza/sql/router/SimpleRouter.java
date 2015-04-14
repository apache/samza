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

package org.apache.samza.sql.router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.router.OperatorRouter;


/**
 * Example implementation of <code>OperatorRouter</code>
 *
 */
public class SimpleRouter implements OperatorRouter {
  /**
   * List of operators added to the <code>OperatorRouter</code>
   */
  private List<Operator> operators = new ArrayList<Operator>();

  @SuppressWarnings("rawtypes")
  /**
   * Map of <code>EntityName</code> to the list of operators associated with it
   */
  private Map<EntityName, List> nextOps = new HashMap<EntityName, List>();

  /**
   * List of <code>EntityName</code> as system inputs
   */
  private List<EntityName> inputEntities = new ArrayList<EntityName>();

  @SuppressWarnings("unchecked")
  private void addOperator(EntityName output, Operator nextOp) {
    if (nextOps.get(output) == null) {
      nextOps.put(output, new ArrayList<Operator>());
    }
    nextOps.get(output).add(nextOp);
    operators.add(nextOp);

  }

  @Override
  public Iterator<Operator> iterator() {
    return operators.iterator();
  }

  @Override
  public void addTupleOperator(EntityName outputStream, TupleOperator nextOp) throws Exception {
    if (!outputStream.isStream()) {
      throw new IllegalArgumentException("Can't attach an TupleOperator " + nextOp.getSpec().getId()
          + " to a non-stream entity " + outputStream);
    }
    addOperator(outputStream, nextOp);
  }

  @Override
  public void addRelationOperator(EntityName outputRelation, RelationOperator nextOp) throws Exception {
    if (!outputRelation.isRelation()) {
      throw new IllegalArgumentException("Can't attach an RelationOperator " + nextOp.getSpec().getId()
          + " to a non-relation entity " + outputRelation);
    }
    addOperator(outputRelation, nextOp);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<RelationOperator> getRelationOperators(EntityName outputRelation) {
    if (!outputRelation.isRelation()) {
      throw new IllegalArgumentException("Can't get RelationOperators for a non-relation output: " + outputRelation);
    }
    return nextOps.get(outputRelation);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<TupleOperator> getTupleOperators(EntityName outputStream) {
    if (!outputStream.isStream()) {
      throw new IllegalArgumentException("Can't get TupleOperators for a non-stream output: " + outputStream);
    }
    return nextOps.get(outputStream);
  }

  @Override
  public boolean hasNextOperators(EntityName output) {
    return nextOps.get(output) != null && !nextOps.get(output).isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Operator> getNextOperators(EntityName output) {
    return nextOps.get(output);
  }

  @Override
  public void addSystemInput(EntityName input) {
    if (!nextOps.containsKey(input) || nextOps.get(input).isEmpty()) {
      throw new IllegalStateException("Can't set a system input w/o any next operators. input:" + input);
    }
    if (!inputEntities.contains(input)) {
      inputEntities.add(input);
    }
  }

  @Override
  public List<EntityName> getSystemInputs() {
    return this.inputEntities;
  }

}
