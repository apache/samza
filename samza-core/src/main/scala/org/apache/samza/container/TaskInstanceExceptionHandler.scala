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

package org.apache.samza.container

import org.apache.samza.config.Config
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.metrics.Counter
import org.apache.samza.metrics.MetricsHelper
import org.apache.samza.util.Logging

/**
 * Handles exceptions thrown in a {@link TaskInstance}'s process or window
 * methods and provides metrics on the number of times ignored exceptions are
 * thrown. The exceptions to ignore are specified using the
 * `task.ignored.exceptions` configuration property.
 *
 * @param metrics The {@link TaskInstanceMetrics} used to track exception
 *        counts.
 * @param ignoredExceptions Set of string names of exception classes to ignore
 *        and count. If the set contains the wildcard "*", then all exceptions
 *        are ignored and counted.
 */
class TaskInstanceExceptionHandler(
  val metrics: MetricsHelper = new TaskInstanceMetrics,
  val ignoredExceptions: Set[String] = Set[String]()) extends Logging {

  val ignoreAll: Boolean = ignoredExceptions.contains("*")
  var counters: Map[String, Counter] = Map[String, Counter]()

  /**
   * Takes a code block and handles any exception thrown in the code block.
   *
   * @param tryCodeBlock The code block to run and handle exceptions from.
   */
  def maybeHandle(tryCodeBlock: => Unit) {
    try {
      tryCodeBlock
    } catch {
      case e: Exception => handle(e)
    }
  }

  /**
   * Handles an exception. If the exception is in the set of exceptions to
   * ignore or if the wildcard is used to ignore all exceptions, then the
   * exception is counted and then ignored. Otherwise, the exception is thrown.
   *
   * @param exception The exception to handle.
   */
  def handle(exception: Exception) {
    val className = exception.getClass.getName
    if (!ignoreAll && !ignoredExceptions.contains(className)) {
      throw exception
    }

    debug("Counting exception " + className)

    counters.get(className) match {
      case Some(counter) => counter.inc()
      case _ => {
        val counter = metrics.newCounter("exception-ignored-" + className)
        counter.inc()
        counters += className -> counter
      }
    }
  }
}

object TaskInstanceExceptionHandler {
  /**
   * Creates a new TaskInstanceExceptionHandler using the provided
   * configuration.
   *
   * @param metrics The {@link TaskInstanceMetrics} used to track exception
   *        counts.
   * @param config The configuration to read the list of ignored exceptions
   *        from.
   */
  def apply(metrics: MetricsHelper, config: Config) =
    new TaskInstanceExceptionHandler(
      metrics = metrics,
      ignoredExceptions = config.getIgnoredExceptions match {
        case Some(exceptions) => exceptions.split(",").toSet
        case _ => Set[String]()
      })
}
