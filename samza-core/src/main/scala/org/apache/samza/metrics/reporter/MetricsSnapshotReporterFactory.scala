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

import org.apache.samza.SamzaException
import org.apache.samza.config._
import org.apache.samza.metrics.{MetricsRegistryMap, MetricsReporter, MetricsReporterFactory}
import org.apache.samza.serializers.{MetricsSnapshotSerdeV2, Serde, SerdeFactory}
import org.apache.samza.system.{SystemFactory, SystemProducer, SystemStream}
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.util.{Logging, ReflectionUtil, StreamUtil, Util}

class MetricsSnapshotReporterFactory extends MetricsReporterFactory with Logging {

  protected def getProducer(reporterName: String, config: Config, registry: MetricsRegistryMap): SystemProducer = {
    val systemConfig = new SystemConfig(config)
    val systemName = getSystemStream(reporterName, config).getSystem
    val systemFactoryClassName = JavaOptionals.toRichOptional(systemConfig.getSystemFactory(systemName)).toOption
      .getOrElse(throw new SamzaException("Trying to fetch system factory for system %s, which isn't defined in config." format systemName))
    val systemFactory = ReflectionUtil.getObj(systemFactoryClassName, classOf[SystemFactory])

    info("Got system factory %s." format systemFactory)
    val producer = systemFactory.getProducer(systemName, config, registry)
    info("Got producer %s." format producer)

    producer
  }

  protected def getSystemStream(reporterName: String, config: Config): SystemStream = {
    val metricsConfig = new MetricsConfig(config)
    val metricsSystemStreamName = JavaOptionals.toRichOptional(metricsConfig.getMetricsSnapshotReporterStream(reporterName))
      .toOption
      .getOrElse(throw new SamzaException("No metrics stream defined in config."))
    val systemStream = StreamUtil.getSystemStreamFromNames(metricsSystemStreamName)
    info("Got system stream %s." format systemStream)
    systemStream
  }

  protected def getSerde(reporterName: String, config: Config): Serde[MetricsSnapshot] = {
    val streamConfig = new StreamConfig(config)
    val systemConfig = new SystemConfig(config)
    val systemStream = getSystemStream(reporterName, config)

    val streamSerdeName = streamConfig.getStreamMsgSerde(systemStream)
    val systemSerdeName = systemConfig.getSystemMsgSerde(systemStream.getSystem)
    val serdeName = streamSerdeName.orElse(systemSerdeName.orElse(null))
    val serializerConfig = new SerializerConfig(config)
    val serde = if (serdeName != null) {
      JavaOptionals.toRichOptional(serializerConfig.getSerdeFactoryClass(serdeName)).toOption match {
        case Some(serdeClassName) =>
          ReflectionUtil.getObj(serdeClassName, classOf[SerdeFactory[MetricsSnapshot]]).getSerde(serdeName, config)
        case _ => null
      }
    } else {
      new MetricsSnapshotSerdeV2
    }
    info("Got serde %s." format serde)
    serde
  }


  protected def getBlacklist(reporterName: String, config: Config): Option[String] = {
    val metricsConfig = new MetricsConfig(config)
    val blacklist = JavaOptionals.toRichOptional(metricsConfig.getMetricsSnapshotReporterBlacklist(reporterName)).toOption
    info("Got blacklist as: %s" format blacklist)
    blacklist
  }

  protected def getReportingInterval(reporterName: String, config: Config): Int = {
    val metricsConfig = new MetricsConfig(config)
    val reportingInterval = metricsConfig.getMetricsSnapshotReporterInterval(reporterName)
    info("Got reporting interval: %d" format reportingInterval)
    reportingInterval
  }

  protected def getJobId(config: Config): String = {
    val jobConfig = new JobConfig(config)
    jobConfig.getJobId
  }

  protected def getJobName(config: Config): String = {
    val jobConfig = new JobConfig(config)
    JavaOptionals.toRichOptional(jobConfig.getName).toOption
      .getOrElse(throw new SamzaException("Job name must be defined in config."))
  }


  def getMetricsReporter(reporterName: String, containerName: String, config: Config): MetricsReporter = {
    info("Creating new metrics snapshot reporter.")
    val registry = new MetricsRegistryMap

    val systemStream = getSystemStream(reporterName, config)
    val producer = getProducer(reporterName, config, registry)
    val reportingInterval = getReportingInterval(reporterName, config);
    val jobName = getJobName(config)
    val jobId = getJobId(config)
    val serde = getSerde(reporterName, config)
    val blacklist = getBlacklist(reporterName, config)

    val reporter = new MetricsSnapshotReporter(
      producer,
      systemStream,
      reportingInterval,
      jobName,
      jobId,
      containerName,
      Util.getTaskClassVersion(config),
      Util.getSamzaVersion,
      Util.getLocalHost.getHostName,
      serde, blacklist)

    reporter.register(this.getClass.getSimpleName, registry)

    reporter
  }
}
