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


public class RandomAccessQueue<T> {
    private T[] m_buffer;
    private int m_capacity;
    private int m_size;
    private int m_head;

    public RandomAccessQueue(Class<T> t, int capacity) {
        m_capacity = capacity;
        m_head = 0;
        m_size = 0;

        @SuppressWarnings("unchecked")
        final T[] buffer =  (T[]) Array.newInstance(t, capacity);
        m_buffer = buffer;
    }

    public synchronized List<T> get(int start, int end) {
        int lowerBound = Math.max(start, 0);
        int upperBound = Math.min(end, m_size - 1);
        List<T> rets = new ArrayList<>();
        for (int i = lowerBound; i <= upperBound; i++) {
            rets.add(m_buffer[(m_head + i) % m_capacity]);
        }
        return rets;
    }

    public synchronized T get(int index) {
        if (index >= 0 && index < m_size) {
            return m_buffer[(m_head + index) % m_capacity];
        }
        throw new CliException("OutOfBoundaryError");
    }

    public synchronized void add(T t) {
        if (m_size >= m_capacity) {
            m_buffer[m_head] = t;
            m_head = (m_head + 1) % m_capacity;
        } else {
            int pos = (m_head + m_size) % m_capacity;
            m_buffer[pos] = t;
            m_size++;
        }
    }

    /*
     * Remove all element before 'end', and return elements between 'start' and 'end'
     */
     public synchronized List<T> consume(int start, int end) {
         List<T> rets = get(start, end);
         int upperBound = Math.min(end, m_size - 1);
         m_head = (end + 1) % m_capacity;
         m_size -= (upperBound + 1);
         return rets;
     }

    public synchronized int getHead() {
        return m_head;
    }

    public synchronized int getSize() {
        return m_size;
    }

    public synchronized void clear() {
        m_head = 0;
        m_size = 0;
    }
}
