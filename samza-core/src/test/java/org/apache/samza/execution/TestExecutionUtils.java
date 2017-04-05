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
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.runtime.AbstractApplicationRunner;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;

public class TestExecutionUtils {

  static ApplicationRunner createRunner(Config config) {
    return new AbstractApplicationRunner(config) {
      @Override
      public void run(StreamApplication streamApp) {
      }

      @Override
      public void kill(StreamApplication streamApp) {

      }

      @Override
      public ApplicationStatus status(StreamApplication streamApp) {
        return null;
      }
    };
  }

  static JoinFunction createJoin() {
    return new JoinFunction() {
      @Override
      public Object apply(Object message, Object otherMessage) {
        return null;
      }

      @Override
      public Object getFirstKey(Object message) {
        return null;
      }

      @Override
      public Object getSecondKey(Object message) {
        return null;
      }
    };
  }

  static SystemAdmin createSystemAdmin(Map<String, Integer> streamToPartitions) {

    return new SystemAdmin() {
      @Override
      public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
        return null;
      }

      @Override
      public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
        Map<String, SystemStreamMetadata> map = new HashMap<>();
        for (String stream : streamNames) {
          Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> m = new HashMap<>();
          for (int i = 0; i < streamToPartitions.get(stream); i++) {
            m.put(new Partition(i), new SystemStreamMetadata.SystemStreamPartitionMetadata("", "", ""));
          }
          map.put(stream, new SystemStreamMetadata(stream, m));
        }
        return map;
      }

      @Override
      public void createChangelogStream(String streamName, int numOfPartitions) {

      }

      @Override
      public void validateChangelogStream(String streamName, int numOfPartitions) {

      }

      @Override
      public void createCoordinatorStream(String streamName) {

      }

      @Override
      public Integer offsetComparator(String offset1, String offset2) {
        return null;
      }
    };
  }
}
