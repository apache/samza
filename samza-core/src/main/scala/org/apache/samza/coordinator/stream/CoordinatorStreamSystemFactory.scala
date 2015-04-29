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

package org.apache.samza.coordinator.stream

import org.apache.samza.SamzaException
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.{Config, SystemConfig}
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.{SystemFactory, SystemStream}
import org.apache.samza.util.Util

/**
 * A helper class that does wiring for CoordinatorStreamSystemConsumer and
 * CoordinatorStreamSystemProducer. This factory should only be used in
 * situations where the underlying SystemConsumer/SystemProducer does not
 * exist.
 */
class CoordinatorStreamSystemFactory {
  def getCoordinatorStreamSystemConsumer(config: Config, registry: MetricsRegistry) = {
    val (coordinatorSystemStream, systemFactory) = Util.getCoordinatorSystemStreamAndFactory(config)
    val systemAdmin = systemFactory.getAdmin(coordinatorSystemStream.getSystem, config)
    val systemConsumer = systemFactory.getConsumer(coordinatorSystemStream.getSystem, config, registry)
    new CoordinatorStreamSystemConsumer(coordinatorSystemStream, systemConsumer, systemAdmin)
  }

  def getCoordinatorStreamSystemProducer(config: Config, registry: MetricsRegistry) = {
    val (coordinatorSystemStream, systemFactory) = Util.getCoordinatorSystemStreamAndFactory(config)
    val systemAdmin = systemFactory.getAdmin(coordinatorSystemStream.getSystem, config)
    val systemProducer = systemFactory.getProducer(coordinatorSystemStream.getSystem, config, registry)
    new CoordinatorStreamSystemProducer(coordinatorSystemStream, systemProducer, systemAdmin)
  }
}