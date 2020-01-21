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

package org.apache.samza.sql.client.exceptions;

/**
 * A CommandHandler throws a CommandHandlerException when it encounters an error.
 */
public class CommandHandlerException extends Exception {

  /**
   * default constructor
   */
  public CommandHandlerException() {
  }

  /**
   * creates instance given error message
   * @param message error message
   */
  public CommandHandlerException(String message) {
    super(message);
  }

  /**
   * creates instance given error message and {@link Throwable} cause
   * @param message error message
   * @param cause throwable cause
   */
  public CommandHandlerException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * creates instance given {@link Throwable} cause
   * @param cause throwable case
   */
  public CommandHandlerException(Throwable cause) {
    super(cause);
  }
}
