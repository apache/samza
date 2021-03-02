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

package org.apache.samza.checkpoint.file

import java.io.File
import java.io.FileNotFoundException
import java.io.FileOutputStream

import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager, CheckpointManagerFactory, CheckpointV1}
import org.apache.samza.config.{Config, FileSystemCheckpointManagerConfig, JobConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.CheckpointV1Serde
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals

import scala.io.Source

class FileSystemCheckpointManager(
                                   jobName: String,
                                   root: File,
                                   serde: CheckpointV1Serde = new CheckpointV1Serde) extends CheckpointManager {

  override def register(taskName: TaskName):Unit = Unit

  def getCheckpointFile(taskName: TaskName) = getFile(jobName, taskName, "checkpoints")

  def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint) {
    val bytes = serde.toBytes(checkpoint.asInstanceOf[CheckpointV1])
    val fos = new FileOutputStream(getCheckpointFile(taskName))

    fos.write(bytes)
    fos.close
  }

  def readLastCheckpoint(taskName: TaskName): Checkpoint = {
    try {
      val bytes = Source.fromFile(getCheckpointFile(taskName)).map(_.toByte).toArray

      serde.fromBytes(bytes)
    } catch {
      case e: FileNotFoundException => null
    }
  }

  def start {
    if (!root.exists) {
      throw new SamzaException("Root directory for file system checkpoint manager does not exist: %s" format root)
    }
  }

  def stop {}

  private def getFile(jobName: String, taskName: TaskName, fileType:String) =
    new File(root, "%s-%s-%s" format (jobName, taskName, fileType))
}

class FileSystemCheckpointManagerFactory extends CheckpointManagerFactory {
  def getCheckpointManager(config: Config, registry: MetricsRegistry) = {
    val jobConfig = new JobConfig(config)
    val name = JavaOptionals.toRichOptional(jobConfig.getName).toOption
      .getOrElse(throw new SamzaException("Missing job name in configs"))
    val fileSystemCheckpointManagerConfig = new FileSystemCheckpointManagerConfig(config)
    val root = JavaOptionals.toRichOptional(fileSystemCheckpointManagerConfig.getFileSystemCheckpointRoot).toOption
      .getOrElse(throw new SamzaException("Missing checkpoint root in configs"))
    new FileSystemCheckpointManager(name, new File(root))
  }
}