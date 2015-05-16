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

package org.apache.samza.sql.operators.join;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.operators.factory.SimpleOperatorImpl;
import org.apache.samza.sql.operators.window.BoundedTimeWindow;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;


/**
 * This class implements a simple stream-to-stream join
 */
public class StreamStreamJoin extends SimpleOperatorImpl {
  private final StreamStreamJoinSpec spec;

  private Map<EntityName, BoundedTimeWindow> inputWindows = new HashMap<EntityName, BoundedTimeWindow>();

  public StreamStreamJoin(StreamStreamJoinSpec spec) {
    super(spec);
    this.spec = spec;
  }

  //TODO: stub constructor to allow compilation pass. Need to construct real StreamStreamJoinSpec.
  public StreamStreamJoin(String opId, List<String> inputRelations, String output, List<String> joinKeys) {
    this(null);
  }

  //TODO: stub constructor to allow compilation pass. Need to construct real StreamStreamJoinSpec.
  public StreamStreamJoin(String opId, List<String> inputRelations, String output, List<String> joinKeys,
      OperatorCallback callback) {
    super(null, callback);
    this.spec = null;
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO Auto-generated method stub
    // initialize the inputWindows map

  }

  private void join(Tuple tuple, Map<EntityName, Stream> joinSets) {
    // TODO Auto-generated method stub
    // Do M-way joins if necessary, it should be ordered based on the orders of the input relations in inputs
    // NOTE: inner joins may be optimized by re-order the input relations by joining inputs w/ less join sets first. We will consider it later.

  }

  private Map<EntityName, Stream> findJoinSets(Tuple tuple) {
    // TODO Auto-generated method stub
    return null;
  }

  private KeyValueIterator<OrderedStoreKey, Tuple> getJoinSet(Tuple tuple, EntityName strmName) {
    // TODO Auto-generated method stub
    return null;
  }

  private List<Entry<String, Object>> getEqualFields(Tuple tuple, EntityName strmName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void realProcess(Relation deltaRelation, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  protected void realProcess(Tuple tuple, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // TODO Auto-generated method stub
    Map<EntityName, Stream> joinSets = findJoinSets(tuple);
    join(tuple, joinSets);
  }

  @Override
  public void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // TODO Auto-generated method stub

  }
}
