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

package org.apache.samza.clustermanager;

import java.time.Duration;
import java.time.Instant;


/**
 * A ProcessorFailure instance encapsulates information related to a Samza processor failure.
 * It keeps track of the time of the last failure, the number of failures.
 */
class ProcessorFailure {
  /**
   * Number of times a processor has failed
   */
  private final int count;
  /**
   * Latest failure time of the processor
   */
  private final Instant lastFailure;

  /**
   * The delay added to the retry attempt of the last failure
   */
  private final Duration lastRetryDelay;

  public ProcessorFailure(int count, Instant lastFailure, Duration lastRetryDelay) {
    this.count = count;
    this.lastFailure = lastFailure;
    this.lastRetryDelay = lastRetryDelay;
  }

  public int getCount() {
    return count;
  }

  public Instant getLastFailure() {
    return lastFailure;
  }

  public Duration getLastRetryDelay() {
    return lastRetryDelay;
  }
}
