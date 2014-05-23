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

package org.apache.samza.system.filereader

import org.apache.samza.system.SystemFactory
import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.SamzaException

class FileReaderSystemFactory extends SystemFactory {
  /**
   * get the FileReaderSystemConsumer. It also tries to get the queue size
   * and polling sleep time from config file. If they do not exist, will use the default
   * value.
   */
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    val queueSize = config.getInt("systems." + systemName + ".queue.size", 10000)
    val pollingSleepMs = config.getInt("systems." + systemName + ".polling.sleep.ms", 500)
    new FileReaderSystemConsumer(systemName, registry, queueSize, pollingSleepMs)
  }

  /**
   * this system is not designed for writing to files. So disable the producer method.
   * It throws Exception when the system tries to getProducer.
   */
  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new SamzaException("not supposed to write to files")
  }

  /**
   * get the FileReaderSystemAdmin
   */
  def getAdmin(systemName: String, config: Config) = {
    new FileReaderSystemAdmin
  }
}