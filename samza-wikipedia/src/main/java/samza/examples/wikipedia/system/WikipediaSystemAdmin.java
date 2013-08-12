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

package samza.examples.wikipedia.system;

import java.util.HashSet;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;

/**
 * Since the Wikipedia system is not inherently partitioned, we can just use a
 * single partition. The StreamTask instance assigned to process messages from
 * partition 0 will receive all events from Wikipedia.
 */
public class WikipediaSystemAdmin implements SystemAdmin {
  private static Set<Partition> ONE_PARTITION = new HashSet<Partition>();

  static {
    ONE_PARTITION.add(new Partition(0));
  }

  @Override
  public Set<Partition> getPartitions(String streamName) {
    return ONE_PARTITION;
  }
}
