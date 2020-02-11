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
package org.apache.samza.clustermanager.utils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * FIFO cache that maintains de-queued container actions. This cache is only accessed by one thread,
 * {@link org.apache.samza.clustermanager.container.placement.ContainerPlacementRequestAllocator} thread in ClusterBasedJobCoordinator
 *
 * This class is not thread-safe
 */
public class BoundedFifoCache<T> {

  private final int cacheSize;
  private final Queue<T> actionQueue;
  private final Set<T> actionCache;

  public BoundedFifoCache(int size) {
    this.actionQueue = new LinkedList<T>();
    this.actionCache = new HashSet<T>();
    this.cacheSize = size;
  }

  public boolean containsKey(T element) {
    return actionCache.contains(element);
  }

  public void put(T element) {
    if (actionCache.size() > cacheSize) {
      T evictedElement = actionQueue.poll();
      actionCache.remove(evictedElement);
    }
    actionCache.add(element);
    actionQueue.add(element);
  }
}
