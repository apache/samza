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

/**
 * Iterates over messages in the provided changelog {@link SystemStreamPartition} using the provided
 * {@link SystemConsumer} until all messages have been consumed.
 *
 * The iterator has a {@link Mode} that depends on its position in the changelog SSP. If trim mode
 * is enabled, the mode switches to {@code TRIM} if the current message offset is greater than the
 * provided {@code restoreOffset}, or if {@code restoreOffset} is null.
 *
 * The iterator mode is used during transactional state restore to determine which changelog SSP entries
 * should be restored and which ones need to be reverted / trimmed from the changelog topic.
 */
public class ChangelogSSPIterator extends BoundedSSPIterator {
  public enum Mode {
    RESTORE,
    TRIM
  }

  private final String restoreOffset;
  private final boolean trimEnabled;
  private Mode mode = Mode.RESTORE;

  public ChangelogSSPIterator(SystemConsumer systemConsumer, SystemStreamPartition systemStreamPartition,
      String restoreOffset, SystemAdmin admin, boolean trimEnabled) {
    this(systemConsumer, systemStreamPartition, restoreOffset, admin, trimEnabled, null);
  }

  // restoreOffset is inclusive when restoring. restoreOffset == null means trim from starting offset to head.
  public ChangelogSSPIterator(SystemConsumer systemConsumer, SystemStreamPartition systemStreamPartition,
      String restoreOffset, SystemAdmin admin, boolean trimEnabled, String endOffset) {
    super(systemConsumer, systemStreamPartition, endOffset, admin);

    this.restoreOffset = restoreOffset;
    this.trimEnabled = trimEnabled;
    if (this.trimEnabled && restoreOffset == null) {
      mode = Mode.TRIM;
    }
  }

  @Override
  public IncomingMessageEnvelope next() {
    IncomingMessageEnvelope envelope = super.next();

    // if trimming changelog is enabled, then switch to trim mode if we've consumed past the restore offset
    // (i.e., restoreOffset was null or current offset is > restoreOffset)
    if (this.trimEnabled && (restoreOffset == null || admin.offsetComparator(envelope.getOffset(), restoreOffset) > 0)) {
      mode = Mode.TRIM;
    }

    return envelope;
  }

  public Mode getMode() {
    return this.mode;
  }
}
