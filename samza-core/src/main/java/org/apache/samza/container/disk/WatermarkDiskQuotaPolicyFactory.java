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

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A factory for producing {@link WatermarkDiskQuotaPolicy} instances. This class is only required
 * due to Samza's string-only config. In the event that a richer config system is added it would
 * be possible to construct a {@link WatermarkDiskQuotaPolicy} directly.
 * <p>
 * <table>
 *   <tr><th>Name</th><th>Default</th><th>Description</th></tr>
 *   <tr>
 *     <td>container.disk.quota.policy.count</td>
 *     <td>0</td>
 *     <td>
 *       The number of entries for this policy. Entries are configured with the following keys by
 *       substituting a 0-based index for each policy. For example, to configure the low water mark
 *       for the first policy to 0.5, use <code>container.disk.quota.policy.0.lowWaterMark=0.5</code>.
 *       Setting this value to 0 disables this policy.
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>container.disk.quota.policy.entry-number.lowWaterMark</td>
 *     <td></td>
 *     <td>
 *       <strong>Required.</strong>
 *       The low water mark for this entry. If the available percentage of disk quota drops below
 *       this low water mark then the work factor for this entry is used unless the available
 *       percentage drops below an entry with an even lower low water mark. For example, if the low
 *       water mark has a value of <code>0.5</code> and the available percentage of disk quota drops
 *       below 50% then the work factor for this entry is used. However, if there were another entry
 *       that had a low water mark of <code>0.4</code> and the disk quota dropped below 40% then
 *       the other entry's work factor would be used instead.
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>container.disk.quota.policy.entry-number.highWaterMark</td>
 *     <td></td>
 *     <td>
 *       <strong>Required.</strong>
 *       The high water mark for this entry. If the available percentage of disk quota rises above
 *       this high water mark then the work factor for this entry is not longer used and the next
 *       highest work factor is used instead. If there is no other higher work factor then the work
 *       factor resets to <code>1.0</code>.
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>container.disk.quota.policy.entry-number.workFactor</td>
 *     <td></td>
 *     <td>
 *       <strong>Required.</strong>
 *       The work factor to apply when this entry is triggered (i.e. due to the available percentage
 *       of disk quota dropping below this entry's low water mark). The work factor is a hint at
 *       the percentage of time that the run loop should be used to process new work. For example,
 *       a value of <code>1.0</code> indicates that the run loop should be working at full rate
 *       (i.e. as fast as it can). A value of <code>0.5</code> means that the run loop should only
 *       run half as fast as it can. A value of <code>0.2</code> means that the run loop should only
 *       run at a 20% of its usual speed.
 *     </td>
 *   </tr>
 * </table>
 */
public class WatermarkDiskQuotaPolicyFactory implements DiskQuotaPolicyFactory {
  private static final Logger log = LoggerFactory.getLogger(WatermarkDiskQuotaPolicyFactory.class);

  private static final String POLICY_COUNT_KEY = "container.disk.quota.policy.count";

  @Override
  public DiskQuotaPolicy create(Config config) {
    final int entryCount = config.getInt(POLICY_COUNT_KEY, 0);
    if (entryCount == 0) {
      log.info("Using a no throttling disk quota policy because policy entry count was missing or set to zero ({})",
          POLICY_COUNT_KEY);
      return new NoThrottlingDiskQuotaPolicy();
    }

    final List<WatermarkDiskQuotaPolicy.Entry> entries = new ArrayList<WatermarkDiskQuotaPolicy.Entry>();
    for (int i = 0; i < entryCount; ++i) {
      final double lowWaterMark = config.getDouble(String.format("container.disk.quota.policy.%d.lowWaterMark", i));
      final double highWaterMark = config.getDouble(String.format("container.disk.quota.policy.%d.highWaterMark", i));
      final double workFactor = config.getDouble(String.format("container.disk.quota.policy.%d.workFactor", i));
      entries.add(new WatermarkDiskQuotaPolicy.Entry(lowWaterMark, highWaterMark, workFactor));
    }

    return new WatermarkDiskQuotaPolicy(entries);
  }
}
