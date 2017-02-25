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

package org.apache.samza.job.yarn

import org.apache.samza.config.{JobConfig, Config}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemFactory

/**
  * A {@link org.apache.samza.system.SystemFactory} implementation that returns a {@link org.apache.samza.job.yarn.MockSystemAdmin}.
  */

class MockSystemFactory extends SystemFactory {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new RuntimeException("Hmm. Not implemented.")
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new RuntimeException("Hmm. Not implemented.")
  }

  def getAdmin(systemName: String, config: Config) = {
    val jobConfig = new JobConfig(config)
    new MockSystemAdmin(jobConfig.getContainerCount)
  }
}
