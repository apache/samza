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
package org.apache.samza.system.chooser

import org.apache.samza.system.{SystemAdmin, SystemStreamPartition}

class MockSystemAdmin extends SystemAdmin {
  override def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = { offsets }
  override def getSystemStreamMetadata(streamNames: java.util.Set[String]) = null

  override def offsetComparator(offset1: String, offset2: String) = {
    offset1.toLong compare offset2.toLong
  }
}
