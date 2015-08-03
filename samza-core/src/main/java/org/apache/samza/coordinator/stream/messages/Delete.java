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

public class Delete extends CoordinatorStreamMessage {
  public Delete(String source, String key, String type) {
    this(source, key, type, VERSION);
  }

  /**
   * <p>
   * Delete messages must take the type of another CoordinatorStreamMessage
   * (e.g. SetConfig) to define the type of message that's being deleted.
   * Considering Kafka's log compaction, for example, the keys of a message
   * and its delete key must match exactly:
   * </p>
   *
   * <pre>
   * k=&gt;[1,"job.name","set-config"] .. v=&gt; {..some stuff..}
   * v=&gt;[1,"job.name","set-config"] .. v=&gt; null
   * </pre>
   *
   * <p>
   * Deletes are modeled as a CoordinatorStreamMessage with a null message
   * map, and a key that's identical to the key map that's to be deleted.
   * </p>
   *
   * @param source  The source ID of the sender of the delete message.
   * @param key     The key to delete.
   * @param type    The type of message to delete. Must correspond to one of hte
   *                other CoordinatorStreamMessages.
   * @param version The protocol version.
   */
  public Delete(String source, String key, String type, int version) {
    super(source);
    setType(type);
    setKey(key);
    setVersion(version);
    setIsDelete(true);
  }
}
