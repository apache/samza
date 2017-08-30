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

package org.apache.samza.config;

import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.apache.samza.SamzaException;
import org.apache.samza.zk.ZkCoordinationUtilsFactory;
import org.junit.Test;


public class TestJobCoordinatorConfig {

  private final static String NONEXISTING_FACTORY_CLASS = "AnotherFactory";
  private final static String ANOTHER_FACTORY_CLASS = TestJobCoordinatorConfig.class.getName(); // any valid name

  @Test
  public void testJobCoordinationUtilsFactoryConfig() {

    Map<String, String> map = new HashMap<>();
    JobCoordinatorConfig jConfig = new JobCoordinatorConfig(new MapConfig(map));

    // test default value
    Assert.assertEquals(ZkCoordinationUtilsFactory.class.getName(), jConfig.getJobCoordinationUtilsFactoryClassName());

    map.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, ANOTHER_FACTORY_CLASS);
    jConfig = new JobCoordinatorConfig(new MapConfig(map));
    Assert.assertEquals(ANOTHER_FACTORY_CLASS, jConfig.getJobCoordinationUtilsFactoryClassName());

    // failure case
    map.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, NONEXISTING_FACTORY_CLASS);
    jConfig = new JobCoordinatorConfig(new MapConfig(map));
    try {
      jConfig.getJobCoordinationUtilsFactoryClassName();
      Assert.fail("Failed to validate loading of fake class: " + NONEXISTING_FACTORY_CLASS);
    } catch (SamzaException e) {
      // expected
    }
  }
}
