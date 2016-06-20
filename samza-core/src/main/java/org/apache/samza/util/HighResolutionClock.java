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
 * An object that can provide time points (useful for getting the elapsed time between two time
 * points) and can sleep for a specified period of time.
 * <p>
 * Instances of this interface must be thread-safe.
 */
interface HighResolutionClock {
  /**
   * Returns a time point that can be used to calculate the difference in nanoseconds with another
   * time point. Resolution of the timer is platform dependent and not guaranteed to actually
   * operate at nanosecond precision.
   *
   * @return current time point in nanoseconds
   */
  long nanoTime();

  /**
   * Sleeps for a period of time that approximates the requested number of nanoseconds. Actual sleep
   * time can vary significantly based on the JVM implementation and platform. This function returns
   * the measured error between expected and actual sleep time.
   *
   * @param nanos the number of nanoseconds to sleep.
   * @throws InterruptedException if the current thread is interrupted while blocked in this method.
   */
  long sleep(long nanos) throws InterruptedException;
}
