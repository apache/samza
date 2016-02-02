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

package org.apache.samza.system.elasticsearch;

import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;
import java.util.Set;

/**
 * Elasticsearch does not make sense to be a changelog store at the moment.
 *
 * <p>All the methods on this class return {@link UnsupportedOperationException}.</p>
 */
public class ElasticsearchSystemAdmin implements SystemAdmin {
  private static final SystemAdmin singleton = new ElasticsearchSystemAdmin();

  private ElasticsearchSystemAdmin() {
    // Ensure this can not be constructed.
  }

  public static SystemAdmin getInstance() {
    return singleton;
  }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(
      Map<SystemStreamPartition, String> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> set) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createChangelogStream(String stream, int foo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createCoordinatorStream(String streamName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void validateChangelogStream(String streamName, int numOfPartitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
	  throw new UnsupportedOperationException();
  }
}
