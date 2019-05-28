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

package org.apache.samza.system.hdfs.writer


import org.apache.hadoop.fs.FileSystem
import org.apache.samza.system.hdfs.HdfsConfig
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.util.ReflectionUtil


object HdfsWriter {
  def getInstance(dfs: FileSystem, systemName: String, samzaConfig: HdfsConfig, classLoader: ClassLoader): HdfsWriter[_] = {
    // instantiate whatever subclass the user configured the system to use
    ReflectionUtil.getObjWithArgs(classLoader, samzaConfig.getHdfsWriterClassName(systemName), classOf[HdfsWriter[_]],
      ReflectionUtil.constructorArgument(dfs, classOf[FileSystem]),
      ReflectionUtil.constructorArgument(systemName, classOf[String]),
      ReflectionUtil.constructorArgument(samzaConfig, classOf[HdfsConfig]))
  }
}

/**
 * Base for all HdfsWriter implementations.
 */
abstract class HdfsWriter[W](dfs: FileSystem, systemName: String, config: HdfsConfig) {
  protected var writer: Option[W] = None

  def flush: Unit

  def write(buffer: OutgoingMessageEnvelope): Unit

  def close: Unit

}

