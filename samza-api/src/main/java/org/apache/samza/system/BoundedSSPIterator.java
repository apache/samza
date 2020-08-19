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

import com.google.common.collect.ImmutableSet;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import org.apache.samza.SamzaException;

/**
 * Iterates over messages in the provided {@link SystemStreamPartition} using the provided
 * {@link SystemConsumer} until all messages with offsets up to and including the {@code endOffset} have been consumed.
 * If {@code endOffset} is null, the iterator will return all messages until caught up to head.
 */
public class BoundedSSPIterator implements Iterator<IncomingMessageEnvelope> {

  protected final SystemAdmin admin;

  private final SystemConsumer systemConsumer;
  private final String endOffset;
  private final Set<SystemStreamPartition> fetchSet;

  private Queue<IncomingMessageEnvelope> peeks;

  public BoundedSSPIterator(SystemConsumer systemConsumer,
      SystemStreamPartition systemStreamPartition, String endOffset, SystemAdmin admin) {
    this.systemConsumer = systemConsumer;
    this.endOffset = endOffset;
    this.admin = admin;
    this.fetchSet = ImmutableSet.of(systemStreamPartition);
    this.peeks = new ArrayDeque<>();
  }

  public boolean hasNext() {
    refresh();

    return peeks.size() > 0 && (endOffset == null || admin.offsetComparator(peeks.peek().getOffset(), endOffset) <= 0);
  }

  public IncomingMessageEnvelope next() {
    refresh();

    if (peeks.size() == 0 || (endOffset != null && admin.offsetComparator(peeks.peek().getOffset(), endOffset) > 0)) {
      throw new NoSuchElementException();
    }

    return peeks.poll();
  }

  private void refresh() {
    if (peeks.size() == 0) {
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
