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


import org.apache.samza.config.{Config, ConfigException, JobConfig}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.hdfs.HdfsSystemConsumer.HdfsSystemConsumerMetrics
import org.apache.samza.util.Logging


class HdfsSystemFactory extends SystemFactory with Logging {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    new HdfsSystemConsumer(systemName, config, new HdfsSystemConsumerMetrics(registry))
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    val jobConfig = new JobConfig(config)
    val jobName = jobConfig.getName.getOrElse(throw new ConfigException("Missing job name."))
    val jobId = jobConfig.getJobId.getOrElse("1")

    val clientId = getClientId("samza-producer", jobName, jobId)
    val metrics = new HdfsSystemProducerMetrics(systemName, registry)
    new HdfsSystemProducer(systemName, clientId, config, metrics)
  }

  def getAdmin(systemName: String, config: Config) = {
    new HdfsSystemAdmin(systemName, config)
  }

  def getClientId(id: String, jobName: String, jobId: String): String = {
    "%s-%s-%s" format
      (id.replaceAll("[^A-Za-z0-9]", "_"),
        jobName.replaceAll("[^A-Za-z0-9]", "_"),
        jobId.replaceAll("[^A-Za-z0-9]", "_"))
  }
}
