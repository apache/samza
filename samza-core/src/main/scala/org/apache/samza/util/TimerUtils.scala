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

package org.apache.samza.util

import org.apache.samza.metrics.Timer

/**
 * a helper class to facilitate update {@link org.apache.samza.metrics.Timer} metric
 */
trait TimerUtils {
  val clock: () => Long

  /**
   * A helper method to update the {@link org.apache.samza.metrics.Timer} metric.
   * It accepts a {@link org.apache.samza.metrics.Timer} instance and a code block.
   * It updates the Timer instance with the duration of running code block.
   */
  def updateTimer[T](timer: Timer)(runCodeBlock: => T): T = {
    val startingTime = clock()
    val returnValue = runCodeBlock
    timer.update(clock() - startingTime)
    returnValue
  }

  /**
   * A helper method to update the {@link org.apache.samza.metrics.Timer} metrics.
   * It accepts a {@link org.apache.samza.metrics.Timer} instance and a code block
   * with no return value. It passes one Long parameter to code block that contains
   * current time in nanoseconds. It updates the Timer instance with the duration of
   * running code block and returns the same duration.
   */
  def updateTimerAndGetDuration(timer: Timer)(runCodeBlock: Long => Unit): Long = {
    val startingTime = clock()
    runCodeBlock(startingTime)
    val duration = clock() - startingTime
    timer.update(duration)
    duration
  }
}