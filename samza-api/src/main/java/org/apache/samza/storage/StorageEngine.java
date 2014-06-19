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

import java.util.Iterator;

import org.apache.samza.system.IncomingMessageEnvelope;

/**
 * A storage engine for managing state maintained by a stream processor.
 * 
 * <p>This interface does not specify any query capabilities, which, of course,
 * would be query engine specific. Instead it just specifies the minimum
 * functionality required to reload a storage engine from its changelog as well
 * as basic lifecycle management.
 */
public interface StorageEngine {

  /**
   * Restore the content of this StorageEngine from the changelog.  Messages are provided
   * in one {@link java.util.Iterator} and not deserialized for efficiency, allowing the
   * implementation to optimize replay, if possible.
   * @param envelopes
   */
  void restore(Iterator<IncomingMessageEnvelope> envelopes);

  /**
   * Flush any cached messages
   */
  void flush();

  /**
   * Close the storage engine
   */
  void stop();

}
