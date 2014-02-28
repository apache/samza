/*
 *
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
 *
 */

package org.apache.samza.util;

import ExponentialThreadSleepStrategy._

/**
 *  Companion object for class ExponentialThreadSleep encapsulating various constants
 */
object ExponentialThreadSleepStrategy {
  val SLEEP_BACK_OFF_MULTIPLIER = 2.0
  val SLEEP_INITIAL_BACK_OFF_DELAY_MS = 100
  val SLEEP_CAP_DELAY_MS = 10000
}

class ExponentialThreadSleepStrategy {

  /**
   * Sleep counter variable used to
   */
  var sleepBackOffMS = 0l

  val sleepMSBackOffDelay = SLEEP_INITIAL_BACK_OFF_DELAY_MS

  val sleepMSCapOnFetchMessagesError = SLEEP_CAP_DELAY_MS

  def sleep() = {
    val nextDelay = getNextDelay(sleepBackOffMS)
    Thread.sleep(nextDelay)
    sleepBackOffMS = nextDelay
  }

  def getNextDelay(previousDelay: Long) = {
    var nextDelay = 0l
    if (previousDelay == 0) {
      nextDelay = sleepMSBackOffDelay
    } else if (SLEEP_BACK_OFF_MULTIPLIER > 1) {
      nextDelay = (previousDelay * SLEEP_BACK_OFF_MULTIPLIER).asInstanceOf[Long]
      if (sleepMSCapOnFetchMessagesError != -1 && nextDelay > sleepMSCapOnFetchMessagesError) {
        nextDelay = scala.math.max(sleepMSCapOnFetchMessagesError, sleepMSBackOffDelay)
      }
    } 
    nextDelay
  }
  
}
