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
package org.apache.samza.util;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * LinkedinHashSet of bounded size {@code size} with a FIFO eviction policy
 *
 * This class is not thread-safe
 */
public class BoundedLinkedHashSet<T> {

  private final int cacheSize;
  private final Set<T> cache;

  public BoundedLinkedHashSet(int size) {
    this.cache = new LinkedHashSet<T>();
    this.cacheSize = size;
  }

  public boolean containsKey(T element) {
    return cache.contains(element);
  }

  public void put(T element) {
    if (cache.size() > cacheSize) {
      Iterator iterator = cache.iterator();
      if (iterator.hasNext()) {
        iterator.remove();
      }
    }
    cache.add(element);
  }
}
