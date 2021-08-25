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

package org.apache.samza.coordinator

import org.apache.samza.config.{Config, _}
import org.apache.samza.container.{LocalityManager, TaskName}
import org.apache.samza.container.grouper.task._
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore
import org.apache.samza.coordinator.server.{HttpServer, JobServlet, LocalityServlet}
import org.apache.samza.coordinator.stream.messages.{SetContainerHostMapping, SetTaskContainerMapping, SetTaskModeMapping, SetTaskPartitionMapping}
import org.apache.samza.job.model.JobModel
import org.apache.samza.metadatastore.MetadataStore
import org.apache.samza.metrics.{MetricsRegistry, MetricsRegistryMap}
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.system._
import org.apache.samza.util.Logging

import java.util
import java.util.concurrent.atomic.AtomicReference

/**
 * Helper companion object that is responsible for wiring up a JobModelManager
 * given a Config object.
 */
object JobModelManager extends Logging {

  val SOURCE = "JobModelManager"

  /**
   * a volatile value to store the current instantiated <code>JobModelManager</code>
   */
  @volatile var currentJobModelManager: JobModelManager = _
  val serializedJobModelRef = new AtomicReference[Array[Byte]]

  /**
   * Currently used only in the ApplicationMaster for yarn deployment model.
   * Does the following:
   * a) Reads the jobModel from coordinator stream using the job's configuration.
   * b) Recomputes the changelog partition mapping based on jobModel and job's configuration.
   * c) Builds JobModelManager using the jobModel read from coordinator stream.
   * @param config config from the coordinator stream.
   * @param changelogPartitionMapping changelog partition-to-task mapping of the samza job.
   * @param metricsRegistry the registry for reporting metrics.
   * @return the instantiated {@see JobModelManager}.
   */
  def apply(config: Config, changelogPartitionMapping: util.Map[TaskName, Integer],
            metadataStore: MetadataStore,
            metricsRegistry: MetricsRegistry = new MetricsRegistryMap()): JobModelManager = {

    // Instantiate the respective metadata store util classes which uses the same coordinator metadata store.
    val localityManager = new LocalityManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetContainerHostMapping.TYPE))
    val taskAssignmentManager = new TaskAssignmentManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskContainerMapping.TYPE), new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskModeMapping.TYPE))
    val taskPartitionAssignmentManager = new TaskPartitionAssignmentManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskPartitionMapping.TYPE))

    val systemAdmins = new SystemAdmins(config, this.getClass.getSimpleName)
    try {
      systemAdmins.start()
      val streamMetadataCache = new StreamMetadataCache(systemAdmins, 0)
      val jobModelHelper = new JobModelHelper(localityManager, taskAssignmentManager, taskPartitionAssignmentManager,
        streamMetadataCache, JobModelCalculator.INSTANCE)
      val jobModel = jobModelHelper.newJobModel(config, changelogPartitionMapping)
      val jobModelToServe = new JobModel(jobModel.getConfig, jobModel.getContainers)
      val serializedJobModelToServe = SamzaObjectMapper.getObjectMapper().writeValueAsBytes(jobModelToServe)
      serializedJobModelRef.set(serializedJobModelToServe)

      val clusterManagerConfig = new ClusterManagerConfig(config)
      val server = new HttpServer(port = clusterManagerConfig.getCoordinatorUrlPort)
      server.addServlet("/", new JobServlet(serializedJobModelRef))
      server.addServlet("/locality", new LocalityServlet(localityManager))

      currentJobModelManager = new JobModelManager(jobModelToServe, server)
      currentJobModelManager
    } finally {
      systemAdmins.stop()
      // Not closing coordinatorStreamStore, since {@code ClusterBasedJobCoordinator} uses it to read container locality through {@code JobModel}.
    }
  }
}

/**
 * <p>JobModelManager is responsible for managing the lifecycle of a Samza job
 * once it's been started. This includes starting and stopping containers,
 * managing configuration, etc.</p>
 *
 * <p>Any new cluster manager that's integrated with Samza (YARN, Mesos, etc)
 * must integrate with the job coordinator.</p>
 *
 * <p>This class' API is currently unstable, and likely to change. The
 * responsibility is simply to propagate the job model, and HTTP
 * server right now.</p>
 */
class JobModelManager(
  /**
   * The data model that describes the Samza job's containers and tasks.
   */
  val jobModel: JobModel,

  /**
   * HTTP server used to serve a Samza job's container model to SamzaContainers when they start up.
   */
  val server: HttpServer = null) extends Logging {

  debug("Got job model: %s." format jobModel)

  def start() {
    if (server != null) {
      debug("Starting HTTP server.")
      server.start
      info("Started HTTP server: %s" format server.getUrl)
    }
  }

  def stop() {
    if (server != null) {
      debug("Stopping HTTP server.")
      server.stop
      info("Stopped HTTP server.")
    }
  }
}
