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

package org.apache.samza.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

public class TestSinglePartitionWithoutOffsetsSystemAdmin {
  @Test
  public void testShouldGetASinglePartition() {
    SinglePartitionWithoutOffsetsSystemAdmin admin = new SinglePartitionWithoutOffsetsSystemAdmin();
    Set<String> streamNames = new HashSet<String>();
    streamNames.add("a");
    streamNames.add("b");

    Map<String, SystemStreamMetadata> metadata = admin.getSystemStreamMetadata(streamNames);
    assertEquals(2, metadata.size());
    SystemStreamMetadata metadata1 = metadata.get("a");
    SystemStreamMetadata metadata2 = metadata.get("b");

    assertEquals(1, metadata1.getSystemStreamPartitionMetadata().size());
    assertEquals(1, metadata2.getSystemStreamPartitionMetadata().size());
    assertNull(metadata.get(new SystemStreamPartition("test-system", "c", new Partition(0))));
  }
}
