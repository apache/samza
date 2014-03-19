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

package org.apache.samza.job.yarn
import org.junit.Assert._
import org.junit.Test
import java.io.BufferedReader
import java.net.URL
import java.io.InputStreamReader
import org.apache.hadoop.yarn.util.ConverterUtils

class TestSamzaAppMasterService {
  @Test
  def testAppMasterDashboardShouldStart {
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "", 1, 2)
    val service = new SamzaAppMasterService(null, state, null, null)

    // start the dashboard
    service.onInit
    assert(state.rpcPort > 0)
    assert(state.trackingPort > 0)

    // check to see if it's running
    val url = new URL("http://127.0.0.1:%d/am" format state.rpcPort)
    val is = url.openConnection().getInputStream();
    val reader = new BufferedReader(new InputStreamReader(is));
    var line: String = null;

    do {
      line = reader.readLine()
    } while (line != null)

    reader.close();
  }
}
