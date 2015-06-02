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

package org.apache.samza.sql.operators.partition;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.operators.factory.SimpleOperatorImpl;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;


/**
 * This is an example build-in operator that performs a simple stream re-partition operation.
 *
 */
public class PartitionOp extends SimpleOperatorImpl {

  /**
   * The specification of this {@code PartitionOp}
   *
   */
  private final PartitionSpec spec;

  /**
   * Ctor that takes the {@link org.apache.samza.sql.operators.partition.PartitionSpec} object as input.
   *
   * @param spec The <code>PartitionSpec</code> object
   */
  public PartitionOp(PartitionSpec spec) {
    super(spec);
    this.spec = spec;
  }

  /**
   * A simplified constructor that allow users to randomly create <code>PartitionOp</code>
   *
   * @param id The identifier of this operator
   * @param input The input stream name of this operator
   * @param system The output system name of this operator
   * @param output The output stream name of this operator
   * @param parKey The partition key used for the output stream
   * @param parNum The number of partitions used for the output stream
   */
  public PartitionOp(String id, String input, String system, String output, String parKey, int parNum) {
    this(new PartitionSpec(id, input, new SystemStream(system, output), parKey, parNum));
  }

  /**
   * A simplified constructor that allow users to randomly create <code>PartitionOp</code>
   *
   * @param id The identifier of this operator
   * @param input The input stream name of this operator
   * @param system The output system name of this operator
   * @param output The output stream name of this operator
   * @param parKey The partition key used for the output stream
   * @param parNum The number of partitions used for the output stream
   * @param callback The callback functions for operator
   */
  public PartitionOp(String id, String input, String system, String output, String parKey, int parNum,
      OperatorCallback callback) {
    super(new PartitionSpec(id, input, new SystemStream(system, output), parKey, parNum), callback);
    this.spec = (PartitionSpec) super.getSpec();
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO Auto-generated method stub
    // No need to initialize store since all inputs are immediately send out
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // TODO Auto-generated method stub
    // NOOP or flush
  }

  @Override
  protected void realProcess(Tuple tuple, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    collector.send(new OutgoingMessageEnvelope(PartitionOp.this.spec.getSystemStream(), tuple.getMessage()
        .getFieldData(PartitionOp.this.spec.getParKey()).value(), tuple.getKey().value(), tuple.getMessage().value()));
  }

  @Override
  protected void realProcess(Relation deltaRelation, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    for(KeyValueIterator<?, Tuple> iter = deltaRelation.all(); iter.hasNext(); ) {
      Entry<?, Tuple> entry = iter.next();
      collector.send(new OutgoingMessageEnvelope(PartitionOp.this.spec.getSystemStream(), entry.getValue().getMessage()
          .getFieldData(PartitionOp.this.spec.getParKey()).value(), entry.getValue().getKey().value(), entry.getValue()
          .getMessage().value()));
    }
  }

}
