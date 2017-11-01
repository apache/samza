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
package org.apache.samza.test.harness
import java.util.{Properties, UUID}

import kafka.consumer.ConsumerConfig
import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, ZkUtils}
import org.apache.kafka.common.security.JaasUtils
import org.apache.samza.system.kafka.{ChangelogInfo, KafkaSystemAdmin}

/**
 * LinkedIn integration test harness for Kafka
 * This is simply a copy of open source code. We do this because java does not support trait and we are making it an
 * abstract class so that user's java test class can extend it.
 */
abstract class AbstractIntegrationTestHarness extends AbstractKafkaServerTestHarness {

  def generateConfigs() =
    TestUtils.createBrokerConfigs(clusterSize(), zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps()))

  /**
   * User can override this method to return the number of brokers they want.
   * By default only one broker will be launched.
   * @return the number of brokers needed in the Kafka cluster for the test.
   */
  def clusterSize(): Int = 1

  /**
   * User can override this method to apply customized configurations to the brokers.
   * By default the only configuration is number of partitions when topics get automatically created. The default value
   * is 1.
   * @return The configurations to be used by brokers.
   */
  def overridingProps(): Properties = {
    val props = new Properties()
    props.setProperty(KafkaConfig.NumPartitionsProp, 1.toString)
    props
  }

  /**
   * Returns the bootstrap servers configuration string to be used by clients.
   * @return bootstrap servers string.
   */
  def bootstrapServers(): String = super.bootstrapUrl

  def createSystemAdmin(system: String): KafkaSystemAdmin = {
    new KafkaSystemAdmin(system, bootstrapServers, connectZk = () => ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, JaasUtils.isZkSecurityEnabled))
  }

}