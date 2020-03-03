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

import org.apache.samza.checkpoint.OffsetManager
import org.apache.samza.scheduler.EpochTimeScheduler
import org.apache.samza.system.{IncomingMessageEnvelope, SystemStreamPartition}
import org.apache.samza.task.{ReadableCoordinator, TaskCallbackFactory}
import org.apache.samza.util.Logging

abstract class RunLoopTask extends Logging {

  val taskName: TaskName

  val isWindowableTask: Boolean = false

  val isAsyncTask: Boolean = false

  val epochTimeScheduler: EpochTimeScheduler = null

  val intermediateStreams: Set[String] = Set()

  val systemStreamPartitions: Set[SystemStreamPartition]

  val metrics: TaskInstanceMetrics

  def process(envelope: IncomingMessageEnvelope, coordinator: ReadableCoordinator,
    callbackFactory: TaskCallbackFactory)

  def endOfStream(coordinator: ReadableCoordinator): Unit = {}

  def window(coordinator: ReadableCoordinator): Unit = {}

  def scheduler(coordinator: ReadableCoordinator): Unit = {}

  def commit: Unit

  def offsetManager: OffsetManager = null
}
