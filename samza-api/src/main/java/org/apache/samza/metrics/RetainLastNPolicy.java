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
package org.apache.samza.metrics;

import java.util.Collection;
import java.util.Iterator;


public class RetainLastNPolicy<T> implements ListGaugeEvictionPolicy<T> {

  protected final ListGauge<T> listGauge;
  protected final int nItems;

  public RetainLastNPolicy(ListGauge<T> listGauge, int numItems) {
    this.listGauge = listGauge;
    this.nItems = numItems;
  }

  @Override
  public void elementAddedCallback() {
    // get a snapshot of the list
    Collection<T> listGaugeCollection = this.listGauge.getValue();
    int numToEvict = listGaugeCollection.size() - nItems;
    Iterator<T> iterator = listGaugeCollection.iterator();
    while (numToEvict > 0 && iterator.hasNext()) {
      // Remove in FIFO order to retain the last nItems
      listGauge.remove(iterator.next());
      numToEvict--;
    }
  }
}
