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

package org.apache.samza.storage.kv;

import java.util.Iterator;

/**
 * An iterator that can be closed.
 *
 * <p>
 * Implement close to free resources assigned to the iterator such as open file handles, persistent state etc.
 *
 * @param <V> the type of value returned by this iterator
 */
public interface ClosableIterator<V> extends Iterator<V> {

  /**
   * Closes this iterator and frees resources assigned to it.
   *
   * It is illegal to invoke {@link #next()} and {@link #hasNext()} after an iterator has been closed.
   */
  public void close();
}
