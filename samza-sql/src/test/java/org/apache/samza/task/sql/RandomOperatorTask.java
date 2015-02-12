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
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.relation.Join;
import org.apache.samza.sql.operators.window.BoundedTimeWindow;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


/***
 * This example illustrate a SQL join operation that joins two streams together using the following operations:
 * <p>a. the two streams are each processed by a window operator to convert to relations
 * <p>b. a join operator is applied on the two relations to generate join results
 * <p>c. finally, the join results are sent out to the system output
 *
 */
public class RandomOperatorTask implements StreamTask, InitableTask, WindowableTask {
  private KeyValueStore<EntityName, List<Object>> opOutputStore;
  private BoundedTimeWindow wndOp1;
  private BoundedTimeWindow wndOp2;
  private Join joinOp;

  private BoundedTimeWindow getWindowOp(EntityName streamName) {
    if (streamName.equals(EntityName.getStreamName("kafka:stream1"))) {
      return this.wndOp1;
    } else if (streamName.equals(EntityName.getStreamName("kafka:stream2"))) {
      return this.wndOp2;
    }

    throw new IllegalArgumentException("No window operator found for stream: " + streamName);
  }

  private void processJoinOutput(List<Object> outputs, MessageCollector collector) {
    // get each tuple in the join operator's outputs and send it to system stream
    for (Object joinOutput : outputs) {
      for (KeyValueIterator<Object, Tuple> iter = ((Relation) joinOutput).all(); iter.hasNext();) {
        Tuple otuple = iter.next().getValue();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "joinOutput1"), otuple.getKey(), otuple
            .getMessage()));
      }
    }
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // create the StoreMessageCollector
    StoreMessageCollector sqlCollector = new StoreMessageCollector(this.opOutputStore);

    // construct the input tuple
    IncomingMessageTuple ituple = new IncomingMessageTuple(envelope);

    // based on tuple's stream name, get the window op and run process()
    BoundedTimeWindow wndOp = getWindowOp(ituple.getStreamName());
    wndOp.process(ituple, sqlCollector);
    List<Object> wndOutputs = sqlCollector.removeOutput(wndOp.getSpec().getOutputNames().get(0));
    if (wndOutputs.isEmpty()) {
      return;
    }

    // process all output from the window operator
    for (Object input : wndOutputs) {
      Relation relation = (Relation) input;
      this.joinOp.process(relation, sqlCollector);
    }
    // get the output from the join operator and send them
    processJoinOutput(sqlCollector.removeOutput(this.joinOp.getSpec().getOutputNames().get(0)), collector);

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // create the StoreMessageCollector
    StoreMessageCollector sqlCollector = new StoreMessageCollector(this.opOutputStore);

    // trigger timeout event on both window operators
    this.wndOp1.window(sqlCollector, coordinator);
    this.wndOp2.window(sqlCollector, coordinator);

    // for all outputs from the window operators, call joinOp.process()
    for (Object input : sqlCollector.removeOutput(this.wndOp1.getSpec().getOutputNames().get(0))) {
      Relation relation = (Relation) input;
      this.joinOp.process(relation, sqlCollector);
    }
    for (Object input : sqlCollector.removeOutput(this.wndOp2.getSpec().getOutputNames().get(0))) {
      Relation relation = (Relation) input;
      this.joinOp.process(relation, sqlCollector);
    }

    // get the output from the join operator and send them
    processJoinOutput(sqlCollector.removeOutput(this.joinOp.getSpec().getOutputNames().get(0)), collector);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // 1. create a fixed length 10 sec window operator
    this.wndOp1 = new BoundedTimeWindow("wndOp1", 10, "kafka:stream1", "relation1");
    this.wndOp2 = new BoundedTimeWindow("wndOp2", 10, "kafka:stream2", "relation2");
    // 2. create a join operation
    List<String> inputRelations = new ArrayList<String>();
    inputRelations.add("relation1");
    inputRelations.add("relation2");
    List<String> joinKeys = new ArrayList<String>();
    joinKeys.add("key1");
    joinKeys.add("key2");
    this.joinOp = new Join("joinOp", inputRelations, "joinOutput", joinKeys);
    // Finally, initialize all operators
    this.opOutputStore =
        (KeyValueStore<EntityName, List<Object>>) context.getStore("samza-sql-operator-output-kvstore");
    this.wndOp1.init(config, context);
    this.wndOp2.init(config, context);
    this.joinOp.init(config, context);
  }

}
