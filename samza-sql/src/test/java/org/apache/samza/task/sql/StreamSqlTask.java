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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.router.OperatorRouter;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.factory.SimpleOperatorFactoryImpl;
import org.apache.samza.sql.operators.partition.PartitionOp;
import org.apache.samza.sql.operators.partition.PartitionSpec;
import org.apache.samza.sql.operators.relation.Join;
import org.apache.samza.sql.operators.relation.JoinSpec;
import org.apache.samza.sql.operators.stream.InsertStream;
import org.apache.samza.sql.operators.stream.InsertStreamSpec;
import org.apache.samza.sql.operators.window.BoundedTimeWindow;
import org.apache.samza.sql.operators.window.WindowSpec;
import org.apache.samza.sql.router.SimpleRouter;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


/***
 * This example illustrate a SQL join operation that joins two streams together using the folowing operations:
 * <ul>
 * <li>a. the two streams are each processed by a window operator to convert to relations
 * <li>b. a join operator is applied on the two relations to generate join results
 * <li>c. an istream operator is applied on join output and convert the relation into a stream
 * <li>d. a partition operator that re-partitions the output stream from istream and send the stream to system output
 * </ul>
 *
 * This example also uses an implementation of <code>SqlMessageCollector</code> (@see <code>OperatorMessageCollector</code>)
 * that uses <code>OperatorRouter</code> to automatically execute the whole paths that connects operators together.
 */
public class StreamSqlTask implements StreamTask, InitableTask, WindowableTask {

  private OperatorRouter rteCntx;

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    SqlMessageCollector opCollector = new OperatorMessageCollector(collector, coordinator, this.rteCntx);

    IncomingMessageTuple ituple = new IncomingMessageTuple(envelope);
    for (Iterator<TupleOperator> iter = this.rteCntx.getTupleOperators(ituple.getStreamName()).iterator(); iter
        .hasNext();) {
      iter.next().process(ituple, opCollector);
    }

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    SqlMessageCollector opCollector = new OperatorMessageCollector(collector, coordinator, this.rteCntx);

    for (EntityName entity : this.rteCntx.getSystemInputs()) {
      for (Iterator<Operator> iter = this.rteCntx.getNextOperators(entity).iterator(); iter.hasNext();) {
        iter.next().window(opCollector, coordinator);
      }
    }

  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // create specification of all operators first
    // 1. create 2 window specifications that define 2 windows of fixed length of 10 seconds
    final WindowSpec spec1 =
        new WindowSpec("fixedWnd1", EntityName.getStreamName("inputStream1"),
            EntityName.getRelationName("fixedWndOutput1"), 10);
    final WindowSpec spec2 =
        new WindowSpec("fixedWnd2", EntityName.getStreamName("inputStream2"),
            EntityName.getRelationName("fixedWndOutput2"), 10);
    // 2. create a join specification that join the output from 2 window operators together
    @SuppressWarnings("serial")
    List<EntityName> inputRelations = new ArrayList<EntityName>() {
      {
        add(spec1.getOutputName());
        add(spec2.getOutputName());
      }
    };
    @SuppressWarnings("serial")
    List<String> joinKeys = new ArrayList<String>() {
      {
        add("key1");
        add("key2");
      }
    };
    JoinSpec joinSpec = new JoinSpec("joinOp", inputRelations, EntityName.getRelationName("joinOutput"), joinKeys);
    // 3. create the specification of an istream operator that convert the output from join to a stream
    InsertStreamSpec istrmSpec =
        new InsertStreamSpec("istremOp", joinSpec.getOutputName(), EntityName.getStreamName("istrmOutput1"));
    // 4. create the specification of a partition operator that re-partitions the stream based on <code>joinKey</code>
    PartitionSpec parSpec =
        new PartitionSpec("parOp1", istrmSpec.getOutputName().getName(), new SystemStream("kafka", "parOutputStrm1"),
            "joinKey", 50);

    // create all operators via the operator factory
    // 1. create two window operators
    SimpleOperatorFactoryImpl operatorFactory = new SimpleOperatorFactoryImpl();
    BoundedTimeWindow wnd1 = (BoundedTimeWindow) operatorFactory.getTupleOperator(spec1);
    BoundedTimeWindow wnd2 = (BoundedTimeWindow) operatorFactory.getTupleOperator(spec2);
    // 2. create one join operator
    Join join = (Join) operatorFactory.getRelationOperator(joinSpec);
    // 3. create one stream operator
    InsertStream istream = (InsertStream) operatorFactory.getRelationOperator(istrmSpec);
    // 4. create a re-partition operator
    PartitionOp par = (PartitionOp) operatorFactory.getTupleOperator(parSpec);

    // Now, connecting the operators via the OperatorRouter
    this.rteCntx = new SimpleRouter();
    // 1. set two system input operators (i.e. two window operators)
    this.rteCntx.addTupleOperator(spec1.getInputName(), wnd1);
    this.rteCntx.addTupleOperator(spec2.getInputName(), wnd2);
    // 2. connect join operator to both window operators
    this.rteCntx.addRelationOperator(spec1.getOutputName(), join);
    this.rteCntx.addRelationOperator(spec2.getOutputName(), join);
    // 3. connect stream operator to the join operator
    this.rteCntx.addRelationOperator(joinSpec.getOutputName(), istream);
    // 4. connect re-partition operator to the stream operator
    this.rteCntx.addTupleOperator(istrmSpec.getOutputName(), par);
    // 5. set the system inputs
    this.rteCntx.addSystemInput(spec1.getInputName());
    this.rteCntx.addSystemInput(spec2.getInputName());

    for (Iterator<Operator> iter = this.rteCntx.iterator(); iter.hasNext();) {
      iter.next().init(config, context);
    }
  }
}
