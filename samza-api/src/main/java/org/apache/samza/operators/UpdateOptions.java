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
package org.apache.samza.operators;

/**
 * This class defines the update contract used in the send-to-table-with-update operator.
 * It tells the operator whether to perform just updates or updates with defaults.
 * */
public enum UpdateOptions {
  /**
   * Indicates that the sendTo operator will only perform an Update operation for the given key.
   * */
  UPDATE_ONLY,
  /**
   * Indicates that the sendTo operator will first apply an update for the given key. If the update fails due to
   * the record not being present for the key, operator will attempt to Put a default and then apply an update.
   * The following conditions need to be true for this to occur:
   * 1) User should provide a default value using {@link UpdateMessage#of(Object, Object)}
   * 2) The Table Integration should throw a {@link org.apache.samza.table.RecordNotFoundException} when update
   * fails due to no record being found for the key.
   * Put a default can fail due to an exception due to various reasons- server error, failed conditional put
   * (failed Put if record already inserted by another process). An update will be re-attempted regardless of the
   * result of the Put default operation.
   * */
  UPDATE_WITH_DEFAULTS;
}
