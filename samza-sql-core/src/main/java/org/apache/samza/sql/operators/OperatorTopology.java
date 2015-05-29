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

import java.util.Iterator;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorSink;
import org.apache.samza.sql.api.operators.OperatorSource;
import org.apache.samza.sql.api.operators.SimpleOperator;


/**
 * This class implements a partially completed {@link org.apache.samza.sql.operators.factory.TopologyBuilder} that signifies a partially completed
 * topology that the current operator has unbounded input stream that can be attached to other operators' output
 */
public class OperatorTopology implements OperatorSource, OperatorSink {

  private final EntityName name;
  private final SimpleRouter router;

  public OperatorTopology(EntityName name, SimpleRouter router) {
    this.name = name;
    this.router = router;
  }

  @Override
  public Iterator<SimpleOperator> opIterator() {
    return this.router.iterator();
  }

  @Override
  public EntityName getName() {
    return this.name;
  }

}
