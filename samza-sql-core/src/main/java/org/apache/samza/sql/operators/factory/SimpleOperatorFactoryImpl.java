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

import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.sql.api.operators.SqlOperatorFactory;
import org.apache.samza.sql.operators.join.StreamStreamJoin;
import org.apache.samza.sql.operators.join.StreamStreamJoinSpec;
import org.apache.samza.sql.operators.partition.PartitionOp;
import org.apache.samza.sql.operators.partition.PartitionSpec;
import org.apache.samza.sql.operators.window.BoundedTimeWindow;
import org.apache.samza.sql.operators.window.WindowSpec;


/**
 * This simple factory class provides method to create the build-in operators per operator specification.
 * It can be extended when the build-in operators expand.
 *
 */
public class SimpleOperatorFactoryImpl implements SqlOperatorFactory {

  @Override
  public SimpleOperator getOperator(OperatorSpec spec) {
    if (spec instanceof PartitionSpec) {
      return new PartitionOp((PartitionSpec) spec);
    } else if (spec instanceof StreamStreamJoinSpec) {
      return new StreamStreamJoin((StreamStreamJoinSpec) spec);
    } else if (spec instanceof WindowSpec) {
      return new BoundedTimeWindow((WindowSpec) spec);
    }
    throw new UnsupportedOperationException("Unsupported operator specified: " + spec.getClass().getCanonicalName());
  }
}
