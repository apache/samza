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

package org.apache.samza.config

import org.apache.samza.system.SystemStream
import TaskConfig._

object DefaultChooserConfig {
  val BOOTSTRAP = StreamConfig.STREAM_PREFIX + "samza.bootstrap"
  val PRIORITY = StreamConfig.STREAM_PREFIX + "samza.priority"
  val BATCH_SIZE = "task.consumer.batch.size"

  implicit def Config2DefaultChooser(config: Config) = new DefaultChooserConfig(config)
}

class DefaultChooserConfig(config: Config) extends ScalaMapConfig(config) {
  import DefaultChooserConfig._

  def getChooserBatchSize = getOption(BATCH_SIZE)

  def getBootstrapStreams = config
    .getInputStreams
    .map(systemStream => (systemStream, getOrElse(BOOTSTRAP format (systemStream.getSystem, systemStream.getStream), "false").equals("true")))
    .filter(_._2.equals("true"))
    .map(_._1)

  def getPriorityStreams = config
    .getInputStreams
    .map(systemStream => (systemStream, getOrElse(PRIORITY format (systemStream.getSystem, systemStream.getStream), "-1").toInt))
    .filter(_._2 >= 0)
    .toMap
}