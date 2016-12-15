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
package org.apache.samza.operators.windows;

import org.apache.samza.operators.data.MessageEnvelope;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The store functions that are used by window and partial join operators to store and retrieve buffered {@link MessageEnvelope}s
 * and partial aggregation results.
 *
 * @param <SK>  the type of key used to store the operator state
 * @param <SS>  the type of operator state. E.g. could be the partial aggregation result for a window, or a buffered
 *             input {@link MessageEnvelope} from the join stream for a join
 */
public class StoreFunctions<M extends MessageEnvelope, SK, SS> {
  /**
   * Function that returns the key to query in the operator state store for a particular {@link MessageEnvelope}.
   * This 1:1 function only returns a single key for the incoming {@link MessageEnvelope}. This is sufficient to support
   * non-overlapping windows and unique-key based joins.
   *
   * TODO: for windows that overlaps (i.e. sliding windows and hopping windows) and non-unique-key-based join,
   * the query to the state store is usually a range scan. We need to add a rangeKeyFinder function
   * (or make this function return a collection) to map from a single input {@link MessageEnvelope} to a range of keys in the store.
   */
  private final Function<M, SK> storeKeyFn;

  /**
   * Function to update the store entry based on the current operator state and the incoming {@link MessageEnvelope}.
   *
   * TODO: this is assuming a 1:1 mapping from the input {@link MessageEnvelope} to the store entry. When implementing sliding/hopping
   * windows and non-unique-key-based join, we may need to include the corresponding state key in addition to the
   * state value. Alternatively this can be called once for each store key for the {@link MessageEnvelope}.
   */
  private final BiFunction<M, SS, SS> stateUpdaterFn;

  public StoreFunctions(Function<M, SK> storeKeyFn, BiFunction<M, SS, SS> stateUpdaterFn) {
    this.storeKeyFn = storeKeyFn;
    this.stateUpdaterFn = stateUpdaterFn;
  }

  public Function<M, SK> getStoreKeyFn() {
    return this.storeKeyFn;
  }

  public BiFunction<M, SS, SS> getStateUpdaterFn() {
    return this.stateUpdaterFn;
  }
}
