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

package org.apache.samza.storage;

import java.util.Collection;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;


/**
 * The processing logic for store side inputs. Accepts incoming messages from side input streams
 * and the current store contents, and returns the new key-value entries to be written to the store.
 */
public interface SideInputProcessor {

  /**
   * Process the incoming side input message for the {@code store}.
   *
   * @param message incoming message envelope
   * @param store key value store associated with the incoming message envelope
   *
   * @return a {@link Collection} of {@link Entry}s that will be written to the {@code store}.
   */
  Collection<Entry<?, ?>> process(IncomingMessageEnvelope message, KeyValueStore store);
}
