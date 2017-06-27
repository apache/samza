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

package org.apache.samza.control;

import org.apache.samza.system.SystemStream;


/**
 * A watermark is a monotonically increasing value, which represents the point up to which the
 * system believes it has received all of the data before the watermark timestamp. Data that arrives
 * with a timestamp that is before the watermark is considered late.
 *
 * <p>This is the aggregate result from the WatermarkManager, which keeps track of the control message
 * {@link org.apache.samza.message.WatermarkMessage} and aggregate by returning the min of all watermark timestamp
 * in each partition.
 */
public interface Watermark {
  /**
   * Returns the timestamp of the watermark
   * Note that if the task consumes more than one partitions of this stream, the watermark emitted is the min of
   * watermarks across all partitions.
   * @return timestamp
   */
  long getTimestamp();

  /**
   * Propagates the watermark to an intermediate stream
   * @param systemStream intermediate stream
   */
  void propagate(SystemStream systemStream);

  /**
   * Create a copy of the watermark with the timestamp
   * @param timestamp new timestamp
   * @return new watermark
   */
  Watermark copyWithTimestamp(long timestamp);
}
