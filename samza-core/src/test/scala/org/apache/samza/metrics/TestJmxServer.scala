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

package org.apache.samza.metrics

import javax.management.remote.JMXConnectorServer
import javax.management.remote.JMXConnectorServerFactory
import org.apache.samza.util.Logging
import org.junit.runner.RunWith
import org.junit.Test
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[JMXConnectorServerFactory]))
@PowerMockIgnore(Array("javax.management.*"))
class TestJmxServer extends Logging {

  @Test
  def serverStartsUp() {
    val mockJMXConnectorServer: JMXConnectorServer = mock(classOf[JMXConnectorServer])

    PowerMockito.mockStatic(classOf[JMXConnectorServerFactory])
    when(JMXConnectorServerFactory.newJMXConnectorServer(anyObject(), anyObject(), anyObject())).thenReturn(mockJMXConnectorServer)

    val jmxServer: JmxServer = new JmxServer
    jmxServer.stop

    verify(mockJMXConnectorServer, atLeastOnce()).start()
    verify(mockJMXConnectorServer, atLeastOnce()).stop()
  }
}
