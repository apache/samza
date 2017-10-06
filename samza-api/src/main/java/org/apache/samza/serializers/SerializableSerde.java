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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A serializer for Serializable objects
 */
public class SerializableSerde<T extends Serializable> implements Serde<T> {

  public byte[] toBytes(T obj) {
    if (obj != null) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = null;
      try {
        oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
      } catch (IOException e) {
        throw new SamzaException("Error writing to output stream", e);
      } finally {
        try {
          if (oos != null) {
            oos.close();
          }
        } catch (IOException e) {
          throw new SamzaException("Error closing output stream", e);
        }
      }

      return bos.toByteArray();
    } else {
      return null;
    }
  }

  public T fromBytes(byte[] bytes) {
    if (bytes != null) {
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      ObjectInputStream ois = null;

      try {
        ois = new ObjectInputStream(bis);
        return (T) ois.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new SamzaException("Error reading from input stream.");
      } finally {
        try {
          if (ois != null) {
            ois.close();
          }
        } catch (IOException e) {
          throw new SamzaException("Error closing input stream", e);
        }
      }
    } else {
      return null;
    }
  }
}
