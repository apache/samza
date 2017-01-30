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
package org.apache.samza.operators.windows;

/**
 * Key for a {@link WindowPane} emitted from a {@link Window}.
 *
 * @param <K> the type of the key in the incoming {@link org.apache.samza.operators.data.MessageEnvelope}.
 *            Windows that are not keyed have a {@link Void} key type.
 *
 */
public class WindowKey<K> {

  private final  K key;

  private final String windowId;

  public WindowKey(K key, String  windowId) {
    this.key = key;
    this.windowId = windowId;
  }

  public K getKey() {
    return key;
  }

  public String getWindowId() {
    return windowId;
  }
}
