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

package org.apache.samza.execution;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(MockitoJUnitRunner.class)
@PrepareForTest({JobPlanner.class})
public class TestJobPlanner {

  @Test
  public void testJobNameIdConfigGeneration() {
    JobPlanner planner = Mockito.mock(JobPlanner.class, Mockito.CALLS_REAL_METHODS);
    Map<String, String> testConfig = new HashMap<>();
    testConfig.put("app.name", "samza-app");
    testConfig .put("app.id", "id");
    planner.generateJobIdAndName(testConfig);
    Assert.assertEquals(testConfig.get("job.name"), "samza-app");
    Assert.assertEquals(testConfig.get("job.id"), "id");
  }

  @Test
  public void testAppConfigPrecedence() {
    JobPlanner planner = Mockito.mock(JobPlanner.class, Mockito.CALLS_REAL_METHODS);
    Map<String, String> testConfig = new HashMap<>();
    testConfig.put("app.name", "samza-app");
    testConfig .put("app.id", "id");
    testConfig .put("job.id", "should-not-exist-id");
    testConfig .put("job.name", "should-not-exist-name");
    planner.generateJobIdAndName(testConfig);
    Assert.assertEquals(testConfig.get("job.name"), "samza-app");
    Assert.assertEquals(testConfig.get("job.id"), "id");
  }

}
