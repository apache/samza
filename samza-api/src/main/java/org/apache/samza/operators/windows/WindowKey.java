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
 * @param <K> the type of the key in the incoming message.
 *            Windows that are not keyed have a {@link Void} key type.
 *
 */
public class WindowKey<K> {

  private final  K key;

  private final String paneId;

  public WindowKey(K key, String  windowId) {
    this.key = key;
    this.paneId = windowId;
  }

  public K getKey() {
    return key;
  }

  public String getPaneId() {
    return paneId;
  }

  @Override
  public String toString() {
    String wndKey = "";
    if (!(key instanceof Void)) {
      wndKey = String.format("%s:", key.toString());
    }
    return String.format("%s%s", wndKey, paneId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    WindowKey<?> windowKey = (WindowKey<?>) o;

    if (!key.equals(windowKey.key)) return false;
    return paneId.equals(windowKey.paneId);

  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + (paneId != null? paneId.hashCode(): 0);
    return result;
  }

}
