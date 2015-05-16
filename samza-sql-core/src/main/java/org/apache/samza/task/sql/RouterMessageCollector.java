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

package org.apache.samza.task.sql;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


/**
 * This class extends {@link org.apache.samza.task.sql.SimpleMessageCollector} that uses {@link org.apache.samza.sql.api.operators.OperatorRouter}
 *
 */
public class RouterMessageCollector extends SimpleMessageCollector {

  private final OperatorRouter rteCntx;

  public RouterMessageCollector(MessageCollector collector, TaskCoordinator coordinator, OperatorRouter rteCntx) {
    super(collector, coordinator);
    this.rteCntx = rteCntx;
  }

  @Override
  protected void realSend(Relation rel) throws Exception {
    for (SimpleOperator op : this.rteCntx.getNextOperators(rel.getName())) {
      op.process(rel, this, coordinator);
    }
  }

  @Override
  protected void realSend(Tuple tuple) throws Exception {
    for (SimpleOperator op : this.rteCntx.getNextOperators(tuple.getEntityName())) {
      op.process(tuple, this, coordinator);
    }
  }
}
