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

package org.apache.samza.container;

import org.apache.samza.coordinator.stream.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.SetContainerHostMapping;

/**
 * Locality Manager is used to persist and read the container-to-host
 * assignment information from the coordinator stream
 * */
public class LocalityManager {
  private static final Logger log = LoggerFactory.getLogger(LocalityManager.class);
  private final CoordinatorStreamSystemConsumer coordinatorStreamConsumer;
  private final CoordinatorStreamSystemProducer coordinatorStreamProducer;
  private static final String SOURCE = "SamzaContainer-";
  private Map<Integer, String> containerToHostMapping;

  public LocalityManager(CoordinatorStreamSystemProducer coordinatorStreamProducer,
                         CoordinatorStreamSystemConsumer coordinatorStreamConsumer) {
    this.coordinatorStreamConsumer = coordinatorStreamConsumer;
    this.coordinatorStreamProducer = coordinatorStreamProducer;
    this.containerToHostMapping = new HashMap<Integer, String>();
  }

  public void start() {
    coordinatorStreamProducer.start();
    coordinatorStreamConsumer.start();
  }

  public void stop() {
    coordinatorStreamConsumer.stop();
    coordinatorStreamProducer.stop();
  }

  /*
   * Register with source suffix that is containerId
   * */
  public void register(String sourceSuffix) {
    coordinatorStreamConsumer.register();
    coordinatorStreamProducer.register(LocalityManager.SOURCE + sourceSuffix);
  }

  public Map<Integer, String> readContainerLocality() {
    Map<Integer, String> allMappings = new HashMap<Integer, String>();
    for (CoordinatorStreamMessage message: coordinatorStreamConsumer.getBootstrappedStream(SetContainerHostMapping.TYPE)) {
      SetContainerHostMapping mapping = new SetContainerHostMapping(message);
      allMappings.put(Integer.parseInt(mapping.getKey()), mapping.getHostLocality());
    }
    containerToHostMapping = Collections.unmodifiableMap(allMappings);
    return allMappings;
  }


  public void writeContainerToHostMapping(Integer containerId, String hostHttpAddress) {
    String existingMapping = containerToHostMapping.get(containerId);
    if (existingMapping != null && !existingMapping.equals(hostHttpAddress)) {
      log.info("Container {} moved from {} to {}", new Object[]{containerId, existingMapping, hostHttpAddress});
    } else {
      log.info("Container {} started at {}", containerId, hostHttpAddress);
    }
    coordinatorStreamProducer.send(new SetContainerHostMapping(SOURCE + containerId, String.valueOf(containerId), hostHttpAddress));
    containerToHostMapping.put(containerId, hostHttpAddress);
  }

}
