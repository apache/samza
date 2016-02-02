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

package org.apache.samza.config;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link org.apache.samza.config.Config} backed by a Java {@link java.util.Map}
 */
public class MapConfig extends Config {
  private final Map<String, String> map;

  public MapConfig() {
    this.map = Collections.emptyMap();
  }

  public MapConfig(Map<String, String> map) {
    this(Collections.singletonList(map));
  }

  public MapConfig(List<Map<String, String>> maps) {
    this.map = new HashMap<String, String>();
    for (Map<String, String> m: maps)
      this.map.putAll(m);
  }

  public String get(Object k) {
    return map.get(k);
  }

  public boolean containsKey(Object k) {
    return map.containsKey(k);
  }

  public Set<Map.Entry<String, String>> entrySet() {
    return map.entrySet();
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public Set<String> keySet() {
    return map.keySet();
  }

  public int size() {
    return map.size();
  }

  public Collection<String> values() {
    return map.values();
  }

  public boolean containsValue(Object v) {
    return map.containsKey(v);
  }

  @Override
  public Config sanitize() {
    return new MapConfig(sanitizeMap());
  }

  private Map<String, String> sanitizeMap() {
    Map<String, String> sanitized = new HashMap<String, String>(map);
    for (Entry<String, String> entry : sanitized.entrySet()) {
      if (entry.getKey().startsWith(SENSITIVE_PREFIX)) {
        entry.setValue(SENSITIVE_MASK);
      }
    }
    return sanitized;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((map == null) ? 0 : map.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MapConfig other = (MapConfig) obj;
    if (map == null) {
      if (other.map != null)
        return false;
    } else if (!map.equals(other.map))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return sanitizeMap().toString();
  }
}
