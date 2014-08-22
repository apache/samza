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

import org.slf4j.LoggerFactory
import org.slf4j.MDC

trait Logging {
  val loggerName = this.getClass.getName
  lazy val logger = LoggerFactory.getLogger(loggerName)

  def trace(message: => Any): Unit = {
    if (logger.isTraceEnabled()) {
      logger.trace(message.toString)
    }
  }

  def trace(message: => Any, e: => Throwable) = {
    if (logger.isTraceEnabled()) {
      logger.trace(message, e)
    }
  }

  def debug(message: => Any): Unit = {
    if (logger.isDebugEnabled()) {
      logger.debug(message.toString)
    }
  }

  def debug(message: => Any, e: => Throwable) = {
    if (logger.isDebugEnabled()) {
      logger.debug(message, e)
    }
  }

  def info(message: => Any): Unit = {
    if (logger.isInfoEnabled()) {
      logger.info(message.toString)
    }
  }

  def info(message: => Any, e: => Throwable) = {
    if (logger.isInfoEnabled()) {
      logger.info(message, e)
    }
  }

  def warn(message: => Any): Unit = {
    logger.warn(message.toString)
  }

  def warn(message: => Any, e: => Throwable) = {
    logger.warn(message, e)
  }

  def error(message: => Any): Unit = {
    logger.error(message.toString)
  }

  def error(message: => Any, e: => Throwable) = {
    logger.error(message, e)
  }

  /**
   * Put a diagnostic context value (the value parameter)
   * and its key into the current thread's diagnostic
   * context map.
   *
   * @param key non-null key
   * @param value value for the key
   *
   * @throws IllegalArgumentException if key is null
   */
  def putMDC(key: => String, value: => String) = {
    MDC.put(key, value)
  }

  /**
   * Get the diagnostic context based on the key.
   *
   * @param key non-null key
   *
   * @throws IllegalArgumentException if key is null
   */
  def getMDC(key: => String) = {
    MDC.get(key)
  }

  /**
   * Remove the diagnostic context identified by the key.
   * This method does not do anything if there is no previous
   * value for this key
   *
   * @param key non-null key
   *
   * @throws IllegalArgumentException if key is null
   */
  def removeMDC(key: => String) = {
    MDC.remove(key)
  }

  /**
   * Clear all the entries in MDC
   */
  def clearMDC = {
    MDC.clear
  }

  /**
   * Converts any type into a String. If the message object is null,
   * returns a "<null>" String. Otherwise, calls "toString" of the
   * object.
   */
  private implicit def _anytype2String(message: Any): String =
    message match {
      case null => "<null>"
      case _ => message.toString
    }
}