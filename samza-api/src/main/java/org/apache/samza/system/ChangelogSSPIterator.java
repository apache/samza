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

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import org.apache.samza.SamzaException;

/**
 * Iterates over messages in the provided changelog {@link SystemStreamPartition} using the provided
 * {@link SystemConsumer} until all messages have been consumed.
 *
 * The iterator has a {@link Mode} that depends on its position in the changelog SSP. If trim mode
 * is enabled, the mode switches to {@code TRIM} if the current message offset is greater than the
 * provided {@code endOffset}, or if {@code endOffset} is null.
 *
 * The iterator mode is used during transactional state restore to determine which changelog SSP entries
 * should be restored and which ones need to be reverted / trimmed from the changelog topic.
 */
public class ChangelogSSPIterator {
  public enum Mode {
    RESTORE,
    TRIM
  }

  private final SystemConsumer systemConsumer;
  private final String endOffset;
  private final SystemAdmin admin;
  private final Set<SystemStreamPartition> fetchSet;
  private final boolean trimEnabled;
  private Queue<IncomingMessageEnvelope> peeks;
  private Mode mode = Mode.RESTORE;

  // endOffset is inclusive when restoring. endOffset == null means trim from staring offset to head.
  public ChangelogSSPIterator(SystemConsumer systemConsumer,
      SystemStreamPartition systemStreamPartition, String endOffset, SystemAdmin admin, boolean trimEnabled) {
    this.systemConsumer = systemConsumer;
    this.endOffset = endOffset;
    this.trimEnabled = trimEnabled;
    if (this.trimEnabled && endOffset == null) {
      mode = Mode.TRIM;
    }
    this.admin = admin;
    this.fetchSet = new HashSet<>();
    this.fetchSet.add(systemStreamPartition);
    this.peeks = new ArrayDeque<>();
  }

  public boolean hasNext() {
    refresh();

    return peeks.size() > 0;
  }

  public IncomingMessageEnvelope next() {
    refresh();

    if (peeks.size() == 0) {
      throw new NoSuchElementException();
    }

    IncomingMessageEnvelope envelope = peeks.poll();

    // if trimming changelog is enabled, then switch to trim mode if if we've consumed past the end offset
    // (i.e., endOffset was null or current offset is > endOffset)
    if (this.trimEnabled && (endOffset == null || admin.offsetComparator(envelope.getOffset(), endOffset) > 0)) {
      mode = Mode.TRIM;
    }

    return envelope;
  }

  public Mode getMode() {
    return this.mode;
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
