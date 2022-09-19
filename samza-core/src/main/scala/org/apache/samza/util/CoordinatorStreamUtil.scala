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
 *
 */

package org.apache.samza.util

import java.util
import org.apache.samza.SamzaException
import org.apache.samza.config._
import org.apache.samza.coordinator.CoordinationConstants
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore
import org.apache.samza.coordinator.stream.{CoordinatorStreamSystemConsumer, CoordinatorStreamSystemProducer, CoordinatorStreamValueSerde}
import org.apache.samza.coordinator.stream.messages.{Delete, SetConfig}
import org.apache.samza.job.JobRunner
import org.apache.samza.metadatastore.MetadataStore
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.{StreamSpec, SystemAdmin, SystemAdmins, SystemFactory, SystemStream}
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals

import scala.collection.JavaConverters._

object CoordinatorStreamUtil extends Logging {
  /**
    * Given a job's full config object, build a subset config which includes
    * only the job name, job id, and system config for the coordinator stream.
    */
  def buildCoordinatorStreamConfig(config: Config): MapConfig = {
    val jobConfig = new JobConfig(config)
    val buildConfigFactory = jobConfig.getCoordinatorStreamFactory
    val coordinatorSystemConfig = Class.forName(buildConfigFactory).newInstance().asInstanceOf[CoordinatorStreamConfigFactory].buildCoordinatorStreamConfig(config)

    new MapConfig(coordinatorSystemConfig)
  }

  /**
   * Creates coordinator stream from config if it does not exist, otherwise no-op.
   *
   * @param config to create coordinator stream.
   */
  def createCoordinatorStream(config: Config): Unit = {
    info("Creating coordinator stream")
    val coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(config)
    val systemConfig = new SystemConfig(config)
    val coordinatorSystemAdmin = systemConfig.getSystemFactories.get(coordinatorSystemStream.getSystem)
      .getAdmin(coordinatorSystemStream.getSystem, config, classOf[DiagnosticsUtil].getSimpleName)

    coordinatorSystemAdmin.start()
    CoordinatorStreamUtil.createCoordinatorStream(coordinatorSystemStream, coordinatorSystemAdmin)
    coordinatorSystemAdmin.stop()
  }

  /**
    * Creates a coordinator stream.
    * @param coordinatorSystemStream the {@see SystemStream} that describes the stream to create.
    * @param coordinatorSystemAdmin the {@see SystemAdmin} used to create the stream.
    */
  def createCoordinatorStream(coordinatorSystemStream: SystemStream, coordinatorSystemAdmin: SystemAdmin): Unit = {
    // TODO: This logic should be part of the final coordinator stream metadata store abstraction. See SAMZA-2182
    val streamName = coordinatorSystemStream.getStream
    val coordinatorSpec = StreamSpec.createCoordinatorStreamSpec(streamName, coordinatorSystemStream.getSystem)
    if (coordinatorSystemAdmin.createStream(coordinatorSpec)) {
      info("Created coordinator stream: %s." format streamName)
    } else {
      info("Coordinator stream: %s already exists." format streamName)
    }
  }

  /**
    * Get the coordinator system stream from the configuration
    * @param config Configuration to get coordinator system stream from.
    * @return
    */
  def getCoordinatorSystemStream(config: Config): SystemStream = {
    val jobConfig = new JobConfig(config)
    val systemName = jobConfig.getCoordinatorSystemName
    val (jobName, jobId) = getJobNameAndId(jobConfig)
    val streamName = getCoordinatorStreamName(jobName, jobId)
    new SystemStream(systemName, streamName)
  }

  /**
    * Get the coordinator system factory from the configuration
    * @param config Configuration to get coordinator system factory from.
    * @return
    */
  def getCoordinatorSystemFactory(config: Config): SystemFactory = {
    val systemName = new JobConfig(config).getCoordinatorSystemName
    val systemConfig = new SystemConfig(config)
    val systemFactoryClassName = JavaOptionals.toRichOptional(systemConfig.getSystemFactory(systemName)).toOption
      .getOrElse(throw new SamzaException("Missing configuration: " + SystemConfig.SYSTEM_FACTORY_FORMAT format systemName))
    ReflectionUtil.getObj(systemFactoryClassName, classOf[SystemFactory])
  }

  /**
    * Generates a coordinator stream name based on the job name and job id
    * for the job. The format of the stream name will be:
    * &#95;&#95;samza_coordinator_&lt;JOBNAME&gt;_&lt;JOBID&gt;.
    */
  def getCoordinatorStreamName(jobName: String, jobId: String): String = {
    "__samza_coordinator_%s_%s" format (jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
  }

  /**
    * Get a job's name and ID given a config. Job ID is defaulted to 1 if not
    * defined in the config, and job name must be defined in config.
    *
    * @return A tuple of (jobName, jobId)
    */
  private def getJobNameAndId(jobConfig: JobConfig) = {
    (JavaOptionals.toRichOptional(jobConfig.getName).toOption
      .getOrElse(throw new ConfigException("Missing required config: job.name")),
      jobConfig.getJobId)
  }

  /**
   * Reads and returns launch config persisted in coordinator stream. Only job.auto sizing configs are currently supported.
   * @param config full job config
   * @param metadataStore an instance of the instantiated MetadataStore
   * @return empty config if auto sizing is disabled, otherwise auto sizing related configs.
   */
  def readLaunchConfigFromCoordinatorStream(config: Config, metadataStore: MetadataStore): Config = {
    if (!config.getBoolean(JobConfig.JOB_AUTOSIZING_ENABLED, false)) {
      new MapConfig()
    } else {
      val config = readConfigFromCoordinatorStream(metadataStore)
      val launchConfig = config.asScala.filterKeys(key => JobConfig.isAutosizingConfig(key)).asJava

      new MapConfig(launchConfig)
    }
  }

  /**
    * Reads and returns the complete configuration stored in the coordinator stream.
    * @param metadataStore an instance of the instantiated {@link CoordinatorStreamStore}.
    * @return the configuration read from the coordinator stream.
    */
  def readConfigFromCoordinatorStream(metadataStore: MetadataStore): Config = {
    val namespaceAwareCoordinatorStreamStore: NamespaceAwareCoordinatorStreamStore = new NamespaceAwareCoordinatorStreamStore(metadataStore, SetConfig.TYPE)
    val configFromCoordinatorStream: util.Map[String, Array[Byte]] = namespaceAwareCoordinatorStreamStore.all
    val configMap: util.Map[String, String] = new util.HashMap[String, String]
    for ((key: String, valueAsBytes: Array[Byte]) <- configFromCoordinatorStream.asScala) {
      if (valueAsBytes == null) {
        warn("Value for key: %s in config is null. Ignoring it." format key)
      } else {
        val valueSerde: CoordinatorStreamValueSerde = new CoordinatorStreamValueSerde(SetConfig.TYPE)
        val valueAsString: String = valueSerde.fromBytes(valueAsBytes)
        if (valueAsString == null) {
          warn("Value for key: %s in config is decoded to be null. Ignoring it." format key)
        } else {
          configMap.put(key, valueAsString)
        }
      }
    }
    new MapConfig(configMap)
  }

  /**
   * Writes config to the metadata store.
   * @param metadataStore an instance of the instantiated {@link CoordinatorStreamStore}.
   * @return the configuration read from the coordinator stream.
   */
  def writeConfigToCoordinatorStream(metadataStore: MetadataStore, config: Config): Unit = {
    val coordinatorStream: NamespaceAwareCoordinatorStreamStore =
      new NamespaceAwareCoordinatorStreamStore(metadataStore, SetConfig.TYPE)
    val valueSerde: CoordinatorStreamValueSerde = new CoordinatorStreamValueSerde(SetConfig.TYPE)
    config.entrySet().forEach((entry) => {
      val key = entry.getKey
      val value = entry.getValue
      if (value == null) {
        warn("Value for key: %s in config is null. Ignoring it." format key)
      } else {
        val valueBytes = valueSerde.toBytes(value)
        if (valueBytes == null) {
          warn("Deserialized value for key: %s in config is null. Ignoring it." format key)
        } else {
          coordinatorStream.put(key, valueBytes);
        }
      }
    })
  }

  def writeConfigToCoordinatorStream(config: Config, resetJobConfig: Boolean = true) {
    debug("config: %s" format config)
    val coordinatorSystemConsumer = new CoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
    val coordinatorSystemProducer = new CoordinatorStreamSystemProducer(config, new MetricsRegistryMap)
    CoordinatorStreamUtil.createCoordinatorStream(config)

    if (resetJobConfig) {
      info("Storing config in coordinator stream.")
      coordinatorSystemProducer.register(JobRunner.SOURCE)
      coordinatorSystemProducer.start()
      coordinatorSystemProducer.writeConfig(JobRunner.SOURCE, config)
    }
    info("Loading old config from coordinator stream.")
    coordinatorSystemConsumer.register()
    coordinatorSystemConsumer.start()
    coordinatorSystemConsumer.bootstrap()
    coordinatorSystemConsumer.stop()

    val oldConfig = coordinatorSystemConsumer.getConfig
    if (resetJobConfig) {
      var keysToRemove = oldConfig.keySet.asScala.toSet.diff(config.keySet.asScala)

      val jobConfig = new JobConfig(config)
      if (jobConfig.getAutosizingEnabled) {
        // If autosizing is enabled, we retain auto-sizing related configs
        keysToRemove = keysToRemove.filter(configKey => !JobConfig.isAutosizingConfig(configKey))
      }

      if (jobConfig.getApplicationMasterHighAvailabilityEnabled) {
        // if AM HA is enabled then retain AM url as running containers are fetching it from c-stream until new AM publishes new AM url.
        keysToRemove = keysToRemove.filter(configKey => !(configKey.equals(CoordinationConstants.YARN_COORDINATOR_URL)))
      }

      info("Deleting old configs that are no longer defined: %s".format(keysToRemove))
      keysToRemove.foreach(key => { coordinatorSystemProducer.send(new Delete(JobRunner.SOURCE, key, SetConfig.TYPE)) })
    }
    coordinatorSystemProducer.stop()
  }
}
