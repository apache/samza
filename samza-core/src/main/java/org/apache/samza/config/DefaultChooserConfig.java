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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.system.SystemStream;


/**
 * A convenience class for fetching configs related to the {@link org.apache.samza.system.chooser.DefaultChooser}
 */
public class DefaultChooserConfig {
  private static final String BATCH_SIZE = "task.consumer.batch.size";

  private final TaskConfigJava taskConfigJava;
  private final StreamConfig streamConfig;

  private final Config config;

  public DefaultChooserConfig(final Config config) {
    if (null == config) {
      throw new IllegalArgumentException("config cannot be null");
    }
    this.config = config;
    taskConfigJava = new TaskConfigJava(config);
    streamConfig = new StreamConfig(config);
  }

  /**
   * @return  the configured batch size, or 0 if it was not configured.
   */
  public int getChooserBatchSize() {
    return config.getInt(BATCH_SIZE, 0);
  }

  /**
   * @return  the set of SystemStreams which were configured as bootstrap streams.
   */
  public Set<SystemStream> getBootstrapStreams() {
    Set<SystemStream> bootstrapInputs = new HashSet<>();
    Set<SystemStream> allInputs = taskConfigJava.getAllInputStreams();
    for (SystemStream systemStream : allInputs) {
      if (streamConfig.getBootstrapEnabled(systemStream)) {
        bootstrapInputs.add(systemStream);
      }
    }
    return Collections.unmodifiableSet(bootstrapInputs);
  }

  /**
   * Gets the priority of every SystemStream for which the priority
   * was explicitly configured with a value &gt;=0.
   *
   * @return  the explicitly-configured stream priorities as a map from
   *          SystemStream to the configured priority value. Streams that
   *          were not explicitly configured with a priority are not returned.
   */
  public Map<SystemStream, Integer> getPriorityStreams() {
    Set<SystemStream> allInputs = taskConfigJava.getAllInputStreams();

    Map<SystemStream, Integer> priorityStreams = new HashMap<>();
    for (SystemStream systemStream : allInputs) {
      int priority = streamConfig.getPriority(systemStream);
      if (priority >= 0) {
        priorityStreams.put(systemStream, priority);
      }
    }
    return Collections.unmodifiableMap(priorityStreams);
  }
}
