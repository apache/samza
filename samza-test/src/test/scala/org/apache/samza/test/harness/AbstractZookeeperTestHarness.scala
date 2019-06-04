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

import kafka.utils.{CoreUtils, Logging, ZkUtils}
import kafka.zk.{EmbeddedZookeeper, KafkaZkClient, ZkFourLetterWords}
import org.junit.{After, Before}
import org.apache.kafka.common.security.JaasUtils
import javax.security.auth.login.Configuration
/**
 * Zookeeper test harness.
 * This is simply a copy of open source Kafka code, we do this because java does not support trait, we are making it abstract
 * class so user java test class can extend it.
 */
abstract class AbstractZookeeperTestHarness extends Logging {

  val zkConnectionTimeout = 60000
  val zkSessionTimeout = 60000

  var zkUtils: ZkUtils = null
  var zookeeper: EmbeddedZookeeper = null

  def zkPort: Int = zookeeper.port

  def zkConnect: String = s"127.0.0.1:$zkPort"

  @Before
  def setUp() {
    zookeeper = new EmbeddedZookeeper()
    /**
      * Change zookeeper session timeout from 6 seconds(default value) to 120 seconds. Saves from the following exception:
      * INFO org.apache.zookeeper.server.ZooKeeperServer - Expiring session 0x162d1cd276b0000, timeout of 6000ms exceeded
      */
    zookeeper.zookeeper.setMinSessionTimeout(120000)
    zookeeper.zookeeper.setMaxSessionTimeout(180000)
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, JaasUtils.isZkSecurityEnabled)
  }

  @After
  def tearDown() {
    if (zkUtils != null)
      CoreUtils.swallow(zkUtils.close(), null)
    if (zookeeper != null)
      CoreUtils.swallow(zookeeper.shutdown(), null)

    def isDown: Boolean = {
      try {
        ZkFourLetterWords.sendStat("127.0.0.1", zkPort, 3000)
        false
      } catch {
        case _: Throwable =>
          debug("Server is down")
          true
      }
    }

    Iterator.continually(isDown).exists(identity)

    Configuration.setConfiguration(null)
  }

}