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

package org.apache.samza.util;

/**
 * An object that performs work and optionally slows the rate of execution. By default,
 * work will not be throttled. Work can be throttled by setting work factor to less than
 * {@link #MAX_WORK_FACTOR}.
 */
public interface Throttleable {
  double MAX_WORK_FACTOR = 1.0;
  double MIN_WORK_FACTOR = 0.001;

  /**
   * Sets the work factor for this object. A work factor of {@code 1.0} indicates that execution
   * should proceed at full throughput. A work factor of less than {@code 1.0} will introduce
   * delays into the execution to approximate the requested work factor. For example, if the
   * work factor is {@code 0.7} then approximately 70% of the execution time will be spent
   * executing the work while 30% will be spent idle.
   *
   * @param workFactor the work factor to set for this throttler.
   */
  void setWorkFactor(double workFactor);

  /**
   * Returns the current work factor in use.
   * @see #setWorkFactor(double)
   * @return the current work factor.
   */
  double getWorkFactor();
}
