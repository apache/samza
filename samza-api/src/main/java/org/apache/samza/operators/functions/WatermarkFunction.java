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

/**
 * Allows user-specific handling of Watermark
 */
public interface WatermarkFunction {

  long WATERMARK_NOT_EXIST = -1;

  /**
   * Processes the input watermark coming from upstream operators.
   * This allows user-defined watermark handling, such as trigger events
   * or propagate it to downstream.
   * @param watermark input watermark
   */
  void processWatermark(long watermark);

  /**
   * Returns the output watermark. This function will be invoked
   * after the transform function (e.g. {@link FlatMapFunction} and the
   * {@link WatermarkFunction#processWatermark(long)} function to collect output watermark.
   *
   * Note: If the transform function returns a collection of output, the output watermark
   * will be emitted after the output collection is propagated to downstream operators. So
   * it might delay the watermark propagation. The delay will cause more buffering and might
   * have performance impact.
   *
   * @return output watermark, WATERMARK_NOT_EXIST is no watermark being created.
   */
  long getOutputWatermark();
}
