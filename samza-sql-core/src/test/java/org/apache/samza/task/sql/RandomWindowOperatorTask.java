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

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.window.BoundedTimeWindow;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


/***
 * This example illustrate a use case for the full-state timed window operator
 *
 */
public class RandomWindowOperatorTask implements StreamTask, InitableTask, WindowableTask {
  private BoundedTimeWindow wndOp;

  private final OperatorCallback wndCallback = new OperatorCallback() {

    @Override
    public Tuple beforeProcess(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator) {
      return tuple;
    }

    @Override
    public Relation beforeProcess(Relation rel, MessageCollector collector, TaskCoordinator coordinator) {
      return rel;
    }

    @Override
    public Relation afterProcess(Relation rel, MessageCollector collector, TaskCoordinator coordinator) {
      return rel;
    }

    @Override
    public Tuple afterProcess(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator) {
      return filterWindowOutput(tuple, collector, coordinator);
    }

    private Tuple filterWindowOutput(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator) {
      // filter all delete tuples before send
      if (tuple.isDelete()) {
        return null;
      }
      return tuple;
    }

  };

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // based on tuple's stream name, get the window op and run process()
    wndOp.process(new IncomingMessageTuple(envelope), collector, coordinator);

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // based on tuple's stream name, get the window op and run process()
    wndOp.refresh(System.nanoTime(), collector, coordinator);
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // 1. create a fixed length 10 sec window operator
    this.wndOp = new BoundedTimeWindow("wndOp1", 10, "kafka:stream1", "relation1", this.wndCallback);
    this.wndOp.init(config, context);
  }
}
