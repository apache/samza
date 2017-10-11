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

package org.apache.samza.system;

import org.apache.samza.SamzaException;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

/**
 * {@link java.util.Iterator} that wraps a
 * {@link org.apache.samza.system.SystemConsumer} to iterate over the messages
 * the consumer provides for the specified
 * {@link org.apache.samza.system.SystemStreamPartition}.
 */
public class SystemStreamPartitionIterator implements Iterator<IncomingMessageEnvelope> {
  private final SystemConsumer systemConsumer;
  private final Set<SystemStreamPartition> fetchSet;
  private Queue<IncomingMessageEnvelope> peeks;
  private boolean endOfStreamReached = false;

  public SystemStreamPartitionIterator(SystemConsumer systemConsumer, SystemStreamPartition systemStreamPartition) {
    this(systemConsumer, systemStreamPartition, 1000);
  }

  public SystemStreamPartitionIterator(SystemConsumer systemConsumer, SystemStreamPartition systemStreamPartition, int fetchSize) {
    this.systemConsumer = systemConsumer;
    this.fetchSet = new HashSet<SystemStreamPartition>();
    this.fetchSet.add(systemStreamPartition);
    this.peeks = new ArrayDeque<IncomingMessageEnvelope>();
  }

  @Override
  public boolean hasNext() {
    refresh();

    return peeks.size() > 0;
  }

  @Override
  public IncomingMessageEnvelope next() {
    refresh();

    if (peeks.size() == 0) {
      throw new NoSuchElementException();
    }

    IncomingMessageEnvelope envelope = peeks.poll();

    if (envelope.isEndOfStream()) {
      endOfStreamReached = true;
    }

    return envelope;
  }

  @Override
  public void remove() {
  }

  private void refresh() {
    if (peeks.size() == 0 && !endOfStreamReached) {
      try {
        Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopes = systemConsumer.poll(fetchSet, SystemConsumer.BLOCK_ON_OUTSTANDING_MESSAGES);

        for (List<IncomingMessageEnvelope> systemStreamPartitionEnvelopes : envelopes.values()) {
          peeks.addAll(systemStreamPartitionEnvelopes);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SamzaException(e);
      }
    }
  }
}
