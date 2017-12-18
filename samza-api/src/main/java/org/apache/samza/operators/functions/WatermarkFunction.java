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

package org.apache.samza.operators.functions;

import java.util.Collection;

/**
 * Allows handling of watermarks.
 */
public interface WatermarkFunction<T> {

  /**
   * Processes the input watermark coming from upstream operators.
   * This allows custom watermark handling, such as triggering events or propagating it downstream.
   *
   * @param watermark input watermark
   * @return output triggered after processing the watermark
   */
  Collection<T> processWatermark(long watermark);

  /**
   * Returns the output watermark. This function will be invoked immediately after either
   * of the following events:
   * <ol>
   *  <li> Return from the transform function, e.g. {@link FlatMapFunction}.
   *  <li> Return from the {@link #processWatermark} function.
   * </ol>

   * Note: If the transform function returns a collection of messages, the output watermark
   * will be emitted after the output collection has been propagated to downstream operators.
   * This might delay the watermark propagation, which will cause more buffering and might
   * have a performance impact.
   *
   * @return output watermark, or null if the output watermark should not be updated.
   *         Samza guarantees that the same watermark value will only be emitted once.
   */
  Long getOutputWatermark();

}
