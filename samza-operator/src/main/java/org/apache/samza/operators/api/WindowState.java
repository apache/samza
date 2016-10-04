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
package org.apache.samza.operators.api;

/**
 * This interface defines the methods a window state class has to implement. The programmers are allowed to implement
 * customized window state to be stored in window state stores by implementing this interface class.
 *
 * @param <WV>  the type for window output value
 */
public interface WindowState<WV> {
  /**
   * Method to get the system time when the first message in the window is received
   *
   * @return  nano-second of system time for the first message received in the window
   */
  long getFirstMessageTimeNs();

  /**
   * Method to get the system time when the last message in the window is received
   *
   * @return  nano-second of system time for the last message received in the window
   */
  long getLastMessageTimeNs();

  /**
   * Method to get the earliest event time in the window
   *
   * @return  the earliest event time in nano-second in the window
   */
  long getEarliestEventTimeNs();

  /**
   * Method to get the latest event time in the window
   *
   * @return  the latest event time in nano-second in the window
   */
  long getLatestEventTimeNs();

  /**
   * Method to get the total number of messages received in the window
   *
   * @return  number of messages in the window
   */
  long getNumberMessages();

  /**
   * Method to get the corresponding window's output value
   *
   * @return  the corresponding window's output value
   */
  WV getOutputValue();

  /**
   * Method to set the corresponding window's output value
   *
   * @param value  the corresponding window's output value
   */
  void setOutputValue(WV value);

}
