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

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;


/**
 * Example implementation of <code>SqlMessageCollector</code> that stores outputs from the operators
 *
 */
public class StoreMessageCollector implements SqlMessageCollector {

  private final KeyValueStore<EntityName, List<Object>> outputStore;

  public StoreMessageCollector(KeyValueStore<EntityName, List<Object>> store) {
    this.outputStore = store;
  }

  @Override
  public void send(Relation deltaRelation) throws Exception {
    saveOutput(deltaRelation.getName(), deltaRelation);
  }

  @Override
  public void send(Tuple tuple) throws Exception {
    saveOutput(tuple.getStreamName(), tuple);
  }

  @Override
  public void timeout(List<EntityName> outputs) throws Exception {
    // TODO Auto-generated method stub
  }

  public List<Object> removeOutput(EntityName id) {
    List<Object> output = outputStore.get(id);
    outputStore.delete(id);
    return output;
  }

  private void saveOutput(EntityName name, Object output) {
    if (this.outputStore.get(name) == null) {
      this.outputStore.put(name, new ArrayList<Object>());
    }
    List<Object> outputs = this.outputStore.get(name);
    outputs.add(output);
  }

  @Override
  public void send(OutgoingMessageEnvelope envelope) {
    saveOutput(
        EntityName.getStreamName(envelope.getSystemStream().getSystem() + ":" + envelope.getSystemStream().getStream()),
        envelope);
  }

}
