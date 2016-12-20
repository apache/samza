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

import org.apache.samza.operators.data.MessageEnvelope;


/**
 * Specifies the result emitted from a {@link Window}.
 *
 * @param <K>  the type of key in the window pane
 * @param <M>  the type of value in the window pane.
 */
public final class WindowPane<K, M> implements MessageEnvelope<K, M> {
  private final K key;
  private final M value;

  WindowPane(K key, M value) {
    this.key = key;
    this.value = value;
  }

  @Override public M getMessage() {
    return this.value;
  }

  @Override public K getKey() {
    return this.key;
  }

  static public <K, M> WindowPane<K, M> of(K key, M result) {
    return new WindowPane<>(key, result);
  }
}


