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

import org.apache.samza.util.{Logging, StreamUtil, Util}
import org.apache.samza.SamzaException
import org.apache.samza.config.{Config, JobConfig, MetricsConfig, SerializerConfig, StreamConfigJava, SystemConfig}
import org.apache.samza.config.StreamConfigJava
import org.apache.samza.metrics.MetricsReporter
import org.apache.samza.metrics.MetricsReporterFactory
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.serializers.{MetricsSnapshotSerdeV2, SerdeFactory}
import org.apache.samza.system.SystemFactory
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals

class MetricsSnapshotReporterFactory extends MetricsReporterFactory with Logging {
  def getMetricsReporter(name: String, containerName: String, config: Config): MetricsReporter = {
    info("Creating new metrics snapshot reporter.")

    val jobConfig = new JobConfig(config)
    val jobName = JavaOptionals.toRichOptional(jobConfig.getName).toOption
      .getOrElse(throw new SamzaException("Job name must be defined in config."))
    val jobId = jobConfig.getJobId

    val metricsConfig = new MetricsConfig(config)
    val metricsSystemStreamName = JavaOptionals.toRichOptional(metricsConfig.getMetricsSnapshotReporterStream(name))
      .toOption
      .getOrElse(throw new SamzaException("No metrics stream defined in config."))

    val systemStream = StreamUtil.getSystemStreamFromNames(metricsSystemStreamName)

    info("Got system stream %s." format systemStream)

    val systemName = systemStream.getSystem

    val systemConfig = new SystemConfig(config)
    val systemFactoryClassName = JavaOptionals.toRichOptional(systemConfig.getSystemFactory(systemName)).toOption
      .getOrElse(throw new SamzaException("Trying to fetch system factory for system %s, which isn't defined in config." format systemName))

    val systemFactory = Util.getObj(systemFactoryClassName, classOf[SystemFactory])

    info("Got system factory %s." format systemFactory)

    val registry = new MetricsRegistryMap

    val producer = systemFactory.getProducer(systemName, config, registry)

    info("Got producer %s." format producer)
    val streamConfig = new StreamConfigJava(config)

    val streamSerdeName = streamConfig.getStreamMsgSerde(systemStream)
    val systemSerdeName = systemConfig.getSystemMsgSerde(systemName)
    val serdeName = streamSerdeName.orElse(systemSerdeName.orElse(null))
    val serializerConfig = new SerializerConfig(config)
    val serde = if (serdeName != null) {
      JavaOptionals.toRichOptional(serializerConfig.getSerdeFactoryClass(serdeName)).toOption match {
        case Some(serdeClassName) =>
          Util.getObj(serdeClassName, classOf[SerdeFactory[MetricsSnapshot]]).getSerde(serdeName, config)
        case _ => null
      }
    } else {
      new MetricsSnapshotSerdeV2
    }

    info("Got serde %s." format serde)

    val pollingInterval: Int = metricsConfig.getMetricsSnapshotReporterInterval(name)

    info("Setting polling interval to %d" format pollingInterval)

    val blacklist = JavaOptionals.toRichOptional(metricsConfig.getMetricsSnapshotReporterBlacklist(name)).toOption
    info("Setting blacklist to %s" format blacklist)

    val reporter = new MetricsSnapshotReporter(
      producer,
      systemStream,
      pollingInterval,
      jobName,
      jobId,
      containerName,
      Util.getTaskClassVersion(config),
      Util.getSamzaVersion(),
      Util.getLocalHost.getHostName,
      serde, blacklist)

    reporter.register(this.getClass.getSimpleName.toString, registry)

    reporter
  }
}
