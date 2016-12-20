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

/**
 * Provides information about a {@Link WindowPane} emitted from a {@link Window}.
 *
 * @param <K> the type of the window key. Windows that are not keyed have a {@link Void} key type.
 *
 */
public class WindowKey<K> {

  private final  K key;

  /**
   * The start time of this window pane.
   */
  private final long windowStart;

  /**
   * The end time of this window pane.
   */
  private final long windowEnd;

  WindowKey(K key, long windowStart, long windowEnd) {
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.key = key;
  }

  public K getKey() {
    return key;
  }

  public long getWindowStart() {
    return windowStart;
  }

  public long getWindowEnd() {
    return windowEnd;
  }
}
