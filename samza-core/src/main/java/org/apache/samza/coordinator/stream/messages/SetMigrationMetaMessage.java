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

package org.apache.samza.coordinator.stream.messages;
/**
 * The Set is used to store the migrations that have been performed
 * The structure looks like:
 * {
 * Key: migration-info
 * Type: set-migration-info
 * Source: ContainerID
 * MessageMap:
 *  {
 *     "0910checkpointmigration" : true
 *  }
 * }
 */
public class SetMigrationMetaMessage extends CoordinatorStreamMessage {
  public static final String TYPE = "set-migration-info";

  public SetMigrationMetaMessage(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  public SetMigrationMetaMessage(String source, String metaInfoKey, String metaInfoVal) {
    super(source);
    setType(TYPE);
    setKey("migration-info");
    putMessageValue(metaInfoKey, metaInfoVal);
  }

  public String getMetaInfo(String metaInfoKey) {
    return getMessageValues().get(metaInfoKey);
  }
}