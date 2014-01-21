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

package org.apache.samza.system;

/**
 * Used to represent a Samza stream.
 */
public class SystemStream {
  protected final String system;
  protected final String stream;

  /**
   * Constructs a Samza stream object from specified components.
   * @param system The name of the system of which this stream is associated with.
   * @param stream The name of the stream as specified in the stream configuration file.
   */
  public SystemStream(String system, String stream) {
    this.system = system;
    this.stream = stream;
  }

  /**
   * Constructs a Samza stream object based upon an existing Samza stream.
   * @param other Reference to an already existing Samza stream.
   */
  public SystemStream(SystemStream other) {
    this(other.getSystem(), other.getStream());
  }

  public String getSystem() {
    return system;
  }

  public String getStream() {
    return stream;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((stream == null) ? 0 : stream.hashCode());
    result = prime * result + ((system == null) ? 0 : system.hashCode());
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
    SystemStream other = (SystemStream) obj;
    if (stream == null) {
      if (other.stream != null)
        return false;
    } else if (!stream.equals(other.stream))
      return false;
    if (system == null) {
      if (other.system != null)
        return false;
    } else if (!system.equals(other.system))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SystemStream [system=" + system + ", stream=" + stream + "]";
  }
}
