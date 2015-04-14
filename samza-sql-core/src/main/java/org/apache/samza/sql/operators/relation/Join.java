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

package org.apache.samza.sql.operators.relation;

import java.util.ArrayList;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.operators.factory.SimpleOperator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SqlMessageCollector;


/**
 * This class defines an example build-in operator for a join operator between two relations.
 *
 */
public class Join extends SimpleOperator implements RelationOperator {

  private final JoinSpec spec;

  /**
   * The input relations
   *
   */
  private List<Relation> inputs = null;

  /**
   * The output relation
   */
  private Relation output = null;

  /**
   * Ctor that creates <code>Join</code> operator based on the specification.
   *
   * @param spec The <code>JoinSpec</code> object that specifies the join operator
   */
  public Join(JoinSpec spec) {
    super(spec);
    this.spec = spec;
  }

  /**
   * An alternative ctor that allows users to create a join operator randomly.
   *
   * @param id The identifier of the join operator
   * @param joinIns The list of input relation names of the join
   * @param joinOut The output relation name of the join
   * @param joinKeys The list of keys used in the join. Each entry in the <code>joinKeys</code> is the key name used in one of the input relations.
   *     The order of the <code>joinKeys</code> MUST be the same as their corresponding relation names in <code>joinIns</code>
   */
  @SuppressWarnings("serial")
  public Join(final String id, final List<String> joinIns, final String joinOut, final List<String> joinKeys) {
    super(new JoinSpec(id, new ArrayList<EntityName>() {
      {
        for (String name : joinIns) {
          add(EntityName.getRelationName(name));
        }
      }
    }, EntityName.getRelationName(joinOut), joinKeys));
    this.spec = (JoinSpec) this.getSpec();
  }

  private boolean hasPendingChanges() {
    return getPendingChanges() != null;
  }

  private Relation getPendingChanges() {
    // TODO Auto-generated method stub
    // return any pending changes that have not been processed yet
    return null;
  }

  private Relation getOutputChanges() {
    // TODO Auto-generated method stub
    return null;
  }

  private boolean hasOutputChanges() {
    // TODO Auto-generated method stub
    return getOutputChanges() != null;
  }

  private void join(Relation deltaRelation) {
    // TODO Auto-generated method stub
    // implement the join logic
    // 1. calculate the delta changes in <code>output</code>
    // 2. check output condition to see whether the current input should trigger an output
    // 3. set the output changes and pending changes
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    for (EntityName relation : this.spec.getInputNames()) {
      inputs.add((Relation) context.getStore(relation.toString()));
    }
    this.output = (Relation) context.getStore(this.spec.getOutputName().toString());
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    SqlMessageCollector sqlCollector = (SqlMessageCollector) collector;
    if (hasPendingChanges()) {
      sqlCollector.send(getPendingChanges());
    }
    sqlCollector.timeout(this.spec.getOutputNames());
  }

  @Override
  public void process(Relation deltaRelation, SqlMessageCollector collector) throws Exception {
    // calculate join based on the input <code>deltaRelation</code>
    join(deltaRelation);
    if (hasOutputChanges()) {
      collector.send(getOutputChanges());
    }
  }
}
