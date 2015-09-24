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

package old.checkpoint

import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.config.Config
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.JobCoordinator
import org.apache.samza.coordinator.stream.messages.{CoordinatorStreamMessage, SetMigrationMetaMessage}
import org.apache.samza.coordinator.stream.{CoordinatorStreamSystemConsumer, CoordinatorStreamSystemProducer, CoordinatorStreamSystemFactory}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.migration.MigrationPlan
import org.apache.samza.storage.ChangelogPartitionManager
import org.apache.samza.util.{Logging, NoOpMetricsRegistry}
import scala.collection.JavaConverters._

class KafkaCheckpointMigration extends MigrationPlan with Logging {
  val source = "CHECKPOINTMIGRATION"
  val migrationKey = "CheckpointMigration09to10"
  val migrationVal = "true"

  def migrate(config: Config, getManager:() => KafkaCheckpointManager): Unit = {
    val coordinatorFactory = new CoordinatorStreamSystemFactory
    val coordinatorSystemProducer = coordinatorFactory.getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap)
    var manager = getManager()
    // make sure to validate that we only perform migration when the checkpoint topic exists
    if (manager.topicExists) {
      manager.validateTopic
      val checkpointMap = manager.readCheckpointsFromLog()
      manager.stop

      manager = getManager()
      val changelogMap = manager.readChangeLogPartitionMapping()
      manager.stop

      val coordinatorSystemConsumer = coordinatorFactory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
      if (migrationVerification(coordinatorSystemConsumer)) {
        info("Migration %s was already performed, doing nothing" format migrationKey)
        return
      }
      info("No previous migration for %s were detected, performing migration" format migrationKey)
      val checkpointManager = new CheckpointManager(coordinatorSystemProducer, coordinatorSystemConsumer, source)
      checkpointManager.start()
      checkpointMap.foreach { case (taskName: TaskName, checkpoint: Checkpoint) => checkpointManager.writeCheckpoint(taskName, checkpoint) }
      val changelogPartitionManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, source)
      changelogPartitionManager.start()
      changelogPartitionManager.writeChangeLogPartitionMapping(changelogMap)
      changelogPartitionManager.stop()
      checkpointManager.stop()
    }
    migrationCompletionMark(coordinatorSystemProducer)
  }

  override def migrate(config: Config) {
    val factory = new KafkaCheckpointManagerFactory
    def getManager() = factory.getCheckpointManager(config, new NoOpMetricsRegistry)
    migrate(config, getManager)
  }

  def migrationVerification(coordinatorSystemConsumer : CoordinatorStreamSystemConsumer): Boolean = {
    coordinatorSystemConsumer.register()
    coordinatorSystemConsumer.start()
    coordinatorSystemConsumer.bootstrap()
    val stream = coordinatorSystemConsumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE)
    coordinatorSystemConsumer.stop()
    val message = new SetMigrationMetaMessage(source, migrationKey, migrationVal)
    stream.contains(message.asInstanceOf[CoordinatorStreamMessage])
  }

  def migrationCompletionMark(coordinatorSystemProducer: CoordinatorStreamSystemProducer) = {
    info("Marking completion of migration %s" format migrationKey)
    val message = new SetMigrationMetaMessage(source, migrationKey, migrationVal)
    coordinatorSystemProducer.start()
    coordinatorSystemProducer.send(message)
    coordinatorSystemProducer.stop()
  }

}
