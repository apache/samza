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

package org.apache.samza.container.disk;

import org.apache.samza.util.ThrottlingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * An object that calculates the current work rate using a configurable set of
 * {@link Entry} instances.
 * <p>
 * The supplied {@link Entry} instances may overlap as long as the range
 * between low and high water mark of one policy entry does not contain the range of low and high
 * water mark of another policy entry. For example, the following entries would be OK:
 *
 * <ul>
 *   <li>Low: 0.5, High: 1.0</li>
 *   <li>Low:0.2, High: 0.8</li>
 * </ul>
 *
 * But the following entries would not:
 *
 * <ul>
 *   <li>Low: 0.1, High: 1.0</li>
 *   <li>Low: 0.2, High: 0.5</li>
 * </ul>
 *
 * Policy entries do not stack. In other words, there is always a clear entry to apply and its work
 * factor is not added, multiplied, or in any other way joined with another entry's work rate.
 */
public class WatermarkDiskQuotaPolicy implements DiskQuotaPolicy {
  /**
   * A comparator that orders {@link Entry} instances in descending order first by
   * high water mark and then by low water mark.
   */
  private static final Comparator<Entry> POLICY_COMPARATOR = new Comparator<Entry>() {
    @Override
    public int compare(Entry lhs, Entry rhs) {
      if (lhs.getHighWaterMarkPercent() > rhs.getHighWaterMarkPercent()) {
        return -1;
      } else if (lhs.getHighWaterMarkPercent() < rhs.getHighWaterMarkPercent()) {
        return 1;
      } else if (lhs.getLowWaterMarkPercent() > rhs.getLowWaterMarkPercent()) {
        return -1;
      } else if (lhs.getLowWaterMarkPercent() < rhs.getLowWaterMarkPercent()) {
        return 1;
      }
      return 0;
    }
  };

  private static final Logger log = LoggerFactory.getLogger(WatermarkDiskQuotaPolicy.class);

  // Lock guards entryIndex
  private final List<Entry> entries;
  private int entryIndex = -1;

  private static String dumpPolicyEntries(List<Entry> entries) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < entries.size(); ++i) {
      final Entry entry = entries.get(i);
      sb.append(String.format("\n    Policy entry %d. Low: %f. High: %f. Work Factor: %f",
          i,
          entry.getLowWaterMarkPercent(),
          entry.getHighWaterMarkPercent(),
          entry.getWorkFactor()));
    }
    return sb.toString();
  }

  /**
   * Constructs a new watermark disk quota policy using the supplied policy entries.
   *
   * @param entries a list of {@link Entry} objects that describe how to adjust the work rate. May
   *                 be empty, but cannot be {@code null}.
   */
  public WatermarkDiskQuotaPolicy(List<Entry> entries) {
    // Copy entries, sort, make immutable
    entries = new ArrayList<>(entries);
    Collections.sort(entries, POLICY_COMPARATOR);
    this.entries = Collections.unmodifiableList(entries);

    // Validate entries
    double lastHighWaterMark = 1.0;
    double lastWorkFactor = ThrottlingExecutor.MAX_WORK_FACTOR;
    for (int i = 0; i < entries.size(); ++i) {
      final Entry entry = entries.get(i);

      if (lastHighWaterMark < entry.getHighWaterMarkPercent()) {
        throw new IllegalArgumentException("Policy entry " + i +
            " has high water mark (" + entry.getHighWaterMarkPercent() +
            ") > previous high water mark (" + lastHighWaterMark + "):" +
            dumpPolicyEntries(entries));
      }

      if (lastWorkFactor < entry.getWorkFactor()) {
        throw new IllegalArgumentException("Policy entry " + i +
            " has work factor (" + entry.getWorkFactor() +
            ") < previous work factor (" + lastWorkFactor + "):" +
            dumpPolicyEntries(entries));
      }

      if (entry.getWorkFactor() < ThrottlingExecutor.MIN_WORK_FACTOR) {
        throw new IllegalArgumentException("Policy entry " + i +
            " has work factor (" + entry.getWorkFactor() +
            ") < minimum work factor (" + ThrottlingExecutor.MIN_WORK_FACTOR + "):" +
            dumpPolicyEntries(entries));
      }

      lastHighWaterMark = entry.getHighWaterMarkPercent();
      lastWorkFactor = entry.getWorkFactor();
    }

    log.info("Using the following disk quota enforcement entries: {}",
        entries.isEmpty() ? "NONE" : dumpPolicyEntries(entries));
  }

  @Override
  public double apply(double availableDiskQuotaPercentage) {
    double workFactor = entryIndex == -1 ? 1.0 : this.entries.get(entryIndex).getWorkFactor();
    int entryIndex = this.entryIndex;

    while (entryIndex >= 0 &&
        entries.get(entryIndex).getHighWaterMarkPercent() <= availableDiskQuotaPercentage) {
      --entryIndex;
    }

    while (entryIndex < entries.size() - 1 &&
        entries.get(entryIndex + 1).getLowWaterMarkPercent() > availableDiskQuotaPercentage) {
      ++entryIndex;
    }

    if (entryIndex != this.entryIndex) {
      workFactor = entryIndex == -1 ? 1.0 : entries.get(entryIndex).getWorkFactor();
      this.entryIndex = entryIndex;
      log.info("Work factor has been updated: {}.", workFactor);
    }

    return workFactor;
  }

  /**
   * A thread-safe value object that represents a policy entry for disk quotas. When the percentage
   * of available disk space assigned via the disk quota drops below the low water mark the
   * configured work rate is applied to reduce throughput of the task. When the available disk space
   * rises above the high water mark the work rate throttling is removed (but other entries may
   * still apply).
   */
  public static class Entry {
    private final double lowWaterMarkPercent;
    private final double highWaterMarkPercent;
    private final double workFactor;

    public Entry(double lowWaterMarkPercent, double highWaterMarkPercent, double workFactor) {
      if (lowWaterMarkPercent < 0.0) {
        throw new IllegalArgumentException("low water mark percent (" + lowWaterMarkPercent + ") < 0");
      }

      if (highWaterMarkPercent > 1.0) {
        throw new IllegalArgumentException("high water mark percent (" + highWaterMarkPercent + ") > 1");
      }

      if (lowWaterMarkPercent > highWaterMarkPercent) {
        throw new IllegalArgumentException("low water mark percent (" + lowWaterMarkPercent + ") > " +
          "high water mark percent (" + highWaterMarkPercent + ")");
      }

      if (workFactor <= 0.0) {
        throw new IllegalArgumentException("work factor (" + workFactor + ") <= 0");
      }

      if (workFactor > 1.0) {
        throw new IllegalArgumentException("work factor (" + workFactor + ") > 1");
      }

      this.lowWaterMarkPercent = lowWaterMarkPercent;
      this.highWaterMarkPercent = highWaterMarkPercent;
      this.workFactor = workFactor;
    }

    public double getLowWaterMarkPercent() {
      return lowWaterMarkPercent;
    }

    public double getHighWaterMarkPercent() {
      return highWaterMarkPercent;
    }

    public double getWorkFactor() {
      return workFactor;
    }
  }
}
