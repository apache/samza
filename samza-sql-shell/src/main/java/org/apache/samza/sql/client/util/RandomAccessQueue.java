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

package org.apache.samza.sql.client.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * A queue that supports random access and consumption.
 * @param <T> Element type
 */
public class RandomAccessQueue<T> {
  private T[] buffer;
  private int capacity;
  private int size;
  private int head;

  public RandomAccessQueue(Class<T> t, int capacity) {
    this.capacity = capacity;
    head = 0;
    size = 0;

    @SuppressWarnings("unchecked") final T[] b = (T[]) Array.newInstance(t, capacity);
    buffer = b;
  }

  public synchronized List<T> get(int start, int end) {
    int lowerBound = Math.max(start, 0);
    int upperBound = Math.min(end, size - 1);
    List<T> rets = new ArrayList<>();
    for (int i = lowerBound; i <= upperBound; i++) {
      rets.add(buffer[(head + i) % capacity]);
    }
    return rets;
  }

  public synchronized T get(int index) {
    if (index >= 0 && index < size) {
      return buffer[(head + index) % capacity];
    }
    throw new CliException("OutOfBoundaryError");
  }

  public synchronized void add(T t) {
    if (size >= capacity) {
      buffer[head] = t;
      head = (head + 1) % capacity;
    } else {
      int pos = (head + size) % capacity;
      buffer[pos] = t;
      size++;
    }
  }

  /*
   * Remove all element before 'end', and return elements between 'start' and 'end'
   */
  public synchronized List<T> consume(int start, int end) {
    List<T> rets = get(start, end);
    int upperBound = Math.min(end, size - 1);
    head = (end + 1) % capacity;
    size -= (upperBound + 1);
    return rets;
  }

  public synchronized int getHead() {
    return head;
  }

  public synchronized int getSize() {
    return size;
  }

  public synchronized void clear() {
    head = 0;
    size = 0;
  }
}
