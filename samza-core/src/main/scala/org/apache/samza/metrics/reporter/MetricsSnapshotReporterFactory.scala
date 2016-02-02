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

package org.apache.samza.metrics.reporter

import org.apache.samza.util.Logging
import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.MetricsConfig.Config2Metrics
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.SerializerConfig.Config2Serializer
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.metrics.MetricsReporter
import org.apache.samza.metrics.MetricsReporterFactory
import org.apache.samza.util.Util
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.serializers.SerdeFactory
import org.apache.samza.system.SystemFactory

class MetricsSnapshotReporterFactory extends MetricsReporterFactory with Logging {
  def getMetricsReporter(name: String, containerName: String, config: Config): MetricsReporter = {
    info("Creating new metrics snapshot reporter.")

    val jobName = config
      .getName
      .getOrElse(throw new SamzaException("Job name must be defined in config."))

    val jobId = config
      .getJobId
      .getOrElse(1.toString)

    val taskClass = config
      .getTaskClass
      .getOrElse(throw new SamzaException("No task class defined for config."))

    val version = Option(Class.forName(taskClass).getPackage.getImplementationVersion)
      .getOrElse({
        warn("Unable to find implementation version in jar's meta info. Defaulting to 0.0.1.")
        "0.0.1"
      })

    val samzaVersion = Option(classOf[MetricsSnapshotReporterFactory].getPackage.getImplementationVersion)
      .getOrElse({
        warn("Unable to find implementation samza version in jar's meta info. Defaulting to 0.0.1.")
        "0.0.1"
      })

    val metricsSystemStreamName = config
      .getMetricsReporterStream(name)
      .getOrElse(throw new SamzaException("No metrics stream defined in config."))

    val systemStream = Util.getSystemStreamFromNames(metricsSystemStreamName)

    info("Got system stream %s." format systemStream)

    val systemName = systemStream.getSystem

    val systemFactoryClassName = config
      .getSystemFactory(systemName)
      .getOrElse(throw new SamzaException("Trying to fetch system factory for system %s, which isn't defined in config." format systemName))

    val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)

    info("Got system factory %s." format systemFactory)

    val registry = new MetricsRegistryMap

    val producer = systemFactory.getProducer(systemName, config, registry)

    info("Got producer %s." format producer)

    val streamSerdeName = config.getStreamMsgSerde(systemStream)
    val systemSerdeName = config.getSystemMsgSerde(systemName)
    val serdeName = streamSerdeName.getOrElse(systemSerdeName.getOrElse(null))
    val serde = if (serdeName != null) {
      config.getSerdeClass(serdeName) match {
        case Some(serdeClassName) =>
          Util
            .getObj[SerdeFactory[MetricsSnapshot]](serdeClassName)
            .getSerde(serdeName, config)
        case _ => null
      }
    } else {
      null
    }

    info("Got serde %s." format serde)

    val pollingInterval: Int = config
      .getMetricsReporterInterval(name)
      .getOrElse("60").toInt

    info("Setting polling interval to %d" format pollingInterval)
    val reporter = new MetricsSnapshotReporter(
      producer,
      systemStream,
      pollingInterval,
      jobName,
      jobId,
      containerName,
      version,
      samzaVersion,
      Util.getLocalHost.getHostName,
      serde)

    reporter.register(this.getClass.getSimpleName.toString, registry)

    reporter
  }
}
