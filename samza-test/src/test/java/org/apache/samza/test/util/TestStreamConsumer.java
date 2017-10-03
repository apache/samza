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

package org.apache.samza.test.util;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

public class TestStreamConsumer implements SystemConsumer {
  private List<IncomingMessageEnvelope> envelopes;

  public TestStreamConsumer(List<IncomingMessageEnvelope> envelopes) {
    this.envelopes = envelopes;
  }

  @Override
  public void start() { }

  @Override
  public void stop() { }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) { }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout)
      throws InterruptedException {
    return systemStreamPartitions.stream().collect(Collectors.toMap(ssp -> ssp, ssp -> envelopes));
  }
}
