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
package org.apache.samza.operators.impl;

/**
 * Wraps the value stored for a particular {@link org.apache.samza.operators.windows.WindowKey} with additional metadata.
 */
class WindowState<WV> {

  private final WV wv;
  /**
   * Time of the first message in the window
   */
  private final long earliestRecvTime;

  WindowState(WV wv, long earliestRecvTime) {
    this.wv = wv;
    this.earliestRecvTime = earliestRecvTime;
  }

  WV getWindowValue() {
    return wv;
  }

  long getEarliestTimestamp() {
    return earliestRecvTime;
  }

  @Override
  public String toString() {
    return String.format("WindowState: {time=%d, value=%s}", earliestRecvTime, wv);
  }
}
