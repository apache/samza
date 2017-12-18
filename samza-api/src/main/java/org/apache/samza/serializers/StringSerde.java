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

package org.apache.samza.serializers;

import org.apache.samza.SamzaException;

import java.io.UnsupportedEncodingException;

/**
 * A serializer for strings
 */
public class StringSerde implements Serde<String> {

  private final String encoding;

  public StringSerde(String encoding) {
    this.encoding = encoding;
  }

  public StringSerde() {
    this("UTF-8");
  }

  public byte[] toBytes(String obj) {
    if (obj != null) {
      try {
        return obj.getBytes(encoding);
      } catch (UnsupportedEncodingException e) {
        throw new SamzaException("Unsupported encoding " + encoding, e);
      }
    } else {
      return null;
    }
  }

  public String fromBytes(byte[] bytes) {
    if (bytes != null) {
      try {
        return new String(bytes, 0, bytes.length, encoding);
      } catch (UnsupportedEncodingException e) {
        throw new SamzaException("Unsupported encoding " + encoding, e);
      }
    } else {
      return null;
    }
  }
}
