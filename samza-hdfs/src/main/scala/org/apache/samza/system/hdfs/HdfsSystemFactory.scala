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

package org.apache.samza.system.hdfs


import org.apache.samza.SamzaException

import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemFactory
import org.apache.samza.util.{KafkaUtil,Logging}


class HdfsSystemFactory extends SystemFactory with Logging {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new SamzaException("HdfsSystemFactory does not implement a consumer")
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    val clientId = KafkaUtil.getClientId("samza-producer", config)
    val metrics = new HdfsSystemProducerMetrics(systemName, registry)
    new HdfsSystemProducer(systemName, clientId, config, metrics)
  }

  def getAdmin(systemName: String, config: Config) = {
    new HdfsSystemAdmin
  }
}
