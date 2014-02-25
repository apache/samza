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

package org.apache.samza.system.chooser;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;

/**
 * MessageChooser is an interface for programmatic fine-grain control over
 * stream consumption.
 * 
 * Consider the case of a Samza task is consuming multiple streams where some
 * streams may be from live systems that have stricter SLA requirements and must
 * always be prioritized over other streams that may be from batch systems.
 * MessageChooser allows developers to inject message prioritization logic into
 * the SamzaContainer.
 * 
 * In general, the MessageChooser can be used to prioritize certain systems,
 * streams or partitions over others. It can also be used to throttle certain
 * partitions if it chooses not to return messages even though they are
 * available when choose is invoked. The MessageChooser can also throttle the
 * entire SamzaContainer by performing a blocking operation, such as
 * Thread.sleep.
 * 
 * The manner in which MessageChooser is used is:
 * 
 * <ul>
 * <li>SystemConsumers buffers messages from all SystemStreamPartitions as they
 * become available.</li>
 * <li>If MessageChooser has no messages for a given SystemStreamPartition, and
 * SystemConsumers has a message in its buffer for the SystemStreamPartition,
 * the MessageChooser will be updated once with the next message in the buffer.</li>
 * <li>When SamzaContainer is ready to process another message, it calls
 * SystemConsumers.choose, which in-turn calls MessageChooser.choose.</li>
 * </ul>
 * 
 * Since the MessageChooser only receives one message at a time per
 * SystemStreamPartition, it can be used to order messages between different
 * SystemStreamPartitions, but it can't be used to re-order messages within a
 * single SystemStreamPartition (a buffered sort). This must be done within a
 * StreamTask.
 * 
 * The contract between the MessageChooser and the SystemConsumers is:
 * 
 * <ul>
 * <li>Update can be called multiple times before choose is called.</li>
 * <li>A null return from MessageChooser.choose means no envelopes should be
 * processed at the moment.</li>
 * <li>A MessageChooser may elect to return null when choose is called, even if
 * unprocessed messages have been given by the update method.</li>
 * <li>A MessageChooser will not have any of its in-memory state restored in the
 * event of a failure.</li>
 * <li>Blocking operations (such as Thread.sleep) will block all processing in
 * the entire SamzaContainer.</li>
 * <li>A MessageChooser should never return the same envelope more than once.</li>
 * <li>Non-deterministic (e.g. time-based) MessageChoosers are allowed.</li>
 * <li>A MessageChooser does not need to be thread-safe.</li>
 * </ul>
 */
public interface MessageChooser {
  /**
   * Called after all SystemStreamPartitions have been registered. Start is used
   * to notify the chooser that it will start receiving update and choose calls.
   */
  void start();

  /**
   * Called when the chooser is about to be discarded. No more messages will be
   * given to the chooser after it is stopped.
   */
  void stop();

  /**
   * Called before start, to let the chooser know that it will be handling
   * envelopes from the given SystemStreamPartition. Register will only be
   * called before start.
   * 
   * @param systemStreamPartition
   *          A SystemStreamPartition that envelopes will be coming from.
   * @param offset
   *          The offset of the first message expected for the
   *          system/stream/partition that's being registered. If "7" were
   *          supplied as the offset, then the MessageChooser can expect the
   *          first message it is updated with for the system/stream/partition
   *          will have an offset of "7".
   */
  void register(SystemStreamPartition systemStreamPartition, String offset);

  /**
   * Notify the chooser that a new envelope is available for a processing.A
   * MessageChooser will receive, at most, one outstanding envelope per
   * system/stream/partition combination. For example, if update is called for
   * partition 7 of kafka.mystream, then update will not be called with an
   * envelope from partition 7 of kafka.mystream until the previous envelope has
   * been returned via the choose method. Update will only be invoked after the
   * chooser has been started.
   * 
   * @param envelope
   *          An unprocessed envelope.
   */
  void update(IncomingMessageEnvelope envelope);

  /**
   * The choose method is invoked when the SamzaContainer is ready to process a
   * new message. The chooser may elect to return any envelope that it's been
   * given via the update method, which hasn't yet been returned. Choose will
   * only be called after the chooser has been started.
   * 
   * @return The next envelope to process, or null if the chooser has no
   *         messages or doesn't want to process any at the moment.
   */
  IncomingMessageEnvelope choose();
}
