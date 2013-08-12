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

import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.samza.Partition;

public class TestSinglePartitionSystemAdmin {
  @Test
  public void testShouldGetASinglePartition() {
    SinglePartitionSystemAdmin admin = new SinglePartitionSystemAdmin();
    Set<Partition> partitions1 = admin.getPartitions("a");
    Set<Partition> partitions2 = admin.getPartitions("b");
    assertEquals(partitions1, partitions2);
    assertEquals(partitions1.size(), 1);
    assertEquals(partitions1.iterator().next(), new Partition(0));
  }
}
