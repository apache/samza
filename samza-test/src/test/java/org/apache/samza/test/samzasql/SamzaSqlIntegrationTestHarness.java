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
package org.apache.samza.test.samzasql;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.util.SamzaSqlTestConfig;
import org.apache.samza.system.MockSystemFactory;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.test.harness.IntegrationTestHarness;
import org.apache.samza.util.CoordinatorStreamUtil;


public class SamzaSqlIntegrationTestHarness extends IntegrationTestHarness {

  public static final String MOCK_METADATA_SYSTEM = "mockmetadatasystem";

  protected void runApplication(Config config) {
    // Use MockSystemFactory for the coordinator system
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition(MOCK_METADATA_SYSTEM,
        CoordinatorStreamUtil.getCoordinatorStreamName(SamzaSqlTestConfig.SQL_JOB, SamzaSqlTestConfig.SQL_JOB_PROCESSOR_ID),
        new Partition(0)), new ArrayList<>());
    HashMap<String, String> mapConfig = new HashMap<>();
    mapConfig.put(JobConfig.JOB_COORDINATOR_SYSTEM, MOCK_METADATA_SYSTEM);
    mapConfig.put(String.format(SystemConfig.SYSTEM_FACTORY_FORMAT, MOCK_METADATA_SYSTEM), MockSystemFactory.class.getName());
    mapConfig.putAll(config);

    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(mapConfig));
    executeRun(runner, config);
    runner.waitForFinish();
  }
}
