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
package org.apache.samza.clustermanager.container.placements;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * FIFO cache that maintains de-queued container actions. This cache is only accessed by one thread,
 * {@link org.apache.samza.clustermanager.container.placement.ContainerPlacementRequestAllocator} thread in ClusterBasedJobCoordinator
 *
 */
public class DequeuedPlacementActionsCache {

  private static final int CACHE_SIZE = 20000;
  private final ConcurrentLinkedQueue<UUID> actionQueue;
  private final Set<UUID> actionCache;

  public DequeuedPlacementActionsCache() {
    this.actionQueue = new ConcurrentLinkedQueue<UUID>();
    this.actionCache = ConcurrentHashMap.newKeySet();
  }

  public boolean containsKey(UUID uuid) {
    return actionCache.contains(uuid);
  }

  public void put(UUID uuid) {
    if (actionCache.size() > CACHE_SIZE) {
      UUID evictedUuid = actionQueue.poll();
      actionCache.remove(evictedUuid);
    }
    actionCache.add(uuid);
    actionQueue.add(uuid);
  }
}
