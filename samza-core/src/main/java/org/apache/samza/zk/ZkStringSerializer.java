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
package org.apache.samza.zk;

import java.io.UnsupportedEncodingException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;


public class ZkStringSerializer implements ZkSerializer {

  private static final String UTF_8 = "UTF-8";

  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError {
    if (data != null) {
      try {
        return ((String) data).getBytes(UTF_8);
      } catch (UnsupportedEncodingException e) {
        throw new ZkMarshallingError(e);
      }
    } else {
      return null;
    }
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError {
    if (bytes != null) {
      try {
        return new String(bytes, 0, bytes.length, UTF_8);
      } catch (UnsupportedEncodingException e) {
        throw new ZkMarshallingError(e);
      }
    } else {
      return null;
    }
  }
}