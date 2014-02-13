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

/**
 * Perform the provided action (via the call method) and, if it returns true,
 * perform it again, the next time.  If, however, call returns false, do not
 * perform call the next time, instead wait 2*n calls before actually calling,
 * with n increasing to the maximum specified in the constructor.

 * @param maxBackOff Absolute maximum number of calls to call before actually performing call.
 */
abstract class DoublingBackOff(maxBackOff:Int = 64) {
  var invocationsBeforeCall = 0
  var currentBackOff = 0

  /**
   * Method to invoke and whose return value will determine the next time 
   * it is called again.
   */
  def call():Boolean

  /**
   * Possibly execute the call method, based on the result of the previous run.
   */
  def maybeCall():Unit = {
   if(invocationsBeforeCall == 0) {
      if (call()) {
        // call succeeded so reset backoff
        currentBackOff = 0
      } else {
        // Call failed, so start backing off
        currentBackOff = scala.math.min(maxBackOff, nextBackOff(currentBackOff))
        invocationsBeforeCall = currentBackOff
      }
    } else {
      invocationsBeforeCall -= 1
    }

  }
  
  // 2 * 0 == 0, making getting started a wee bit hard, so we need a little help with that first back off
  private def nextBackOff(i:Int) = if(i == 0) 1 else 2 * i

}

