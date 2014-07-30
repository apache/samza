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
package org.apache.samza.container.grouper.stream

import org.apache.samza.container.TaskName
import scala.collection.JavaConverters._
import org.junit.Test

class TestGroupBySystemStreamPartition extends GroupByTestBase {
  import GroupByTestBase._

  // Building manually to avoid just duplicating a logic potential logic error here and there
  val expected /* from base class provided set */ =  Map(new TaskName(aa0.toString) -> Set(aa0).asJava,
    new TaskName(aa1.toString) -> Set(aa1).asJava,
    new TaskName(aa2.toString) -> Set(aa2).asJava,
    new TaskName(ab1.toString) -> Set(ab1).asJava,
    new TaskName(ab2.toString) -> Set(ab2).asJava,
    new TaskName(ac0.toString) -> Set(ac0).asJava).asJava

  override def getGrouper: SystemStreamPartitionGrouper = new GroupBySystemStreamPartition

  @Test def groupingWorks() {
    verifyGroupGroupsCorrectly(allSSPs, expected)
  }
}
