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
package org.apache.samza.job.yarn.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.samza.config.Config;
import org.apache.samza.job.yarn.ContainerUtil;
import org.apache.samza.job.yarn.SamzaAppState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class MockContainerUtil extends ContainerUtil {
  public final Map<String, List<Container>> runningContainerList = new HashMap<>();

  public MockContainerUtil(Config config, SamzaAppState state, YarnConfiguration conf, NMClient nmClient) {
    super(config, state, conf);
    this.setNmClient(nmClient);
  }

  @Override
  public void runContainer(int samzaContainerId, Container container) {
    String hostname = container.getNodeHttpAddress().split(":")[0];
    List<Container> list = runningContainerList.get(hostname);
    if (list == null) {
      list = new ArrayList<Container>();
      list.add(container);
      runningContainerList.put(hostname, list);
    } else {
      list.add(container);
      runningContainerList.put(hostname, list);
    }
    super.runContainer(samzaContainerId, container);
  }

  @Override
  public void startContainer(Path packagePath, Container container, Map<String, String> env, String cmd) {
  }

}
