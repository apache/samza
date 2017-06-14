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
 * A watermark is a monotonically increasing value, which represents the point up to which the
 * system believes it has received all of the data before the watermark timestamp. Data that arrives
 * with a timestamp that is before the watermark is considered late.
 *
 * <p>This class defines the watermark object in the {@link IncomingMessageEnvelope#getMessage()}
 * It returns the next watermark timestamp from a stream.
 */
public interface Watermark {
  /**
   * Returns the timestamp of the watermark
   * @return timestamp
   */
  long getTimestamp();

  /**
   * Returns the {@link SystemStream} that generates the watermark.
   * @return stream of the watermark
   */
  SystemStream getSystemStream();
}
