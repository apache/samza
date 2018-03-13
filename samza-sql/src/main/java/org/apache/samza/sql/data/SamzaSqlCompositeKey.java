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

package org.apache.samza.sql.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A serializable class that holds different key parts.
 */
public class SamzaSqlCompositeKey implements Serializable {

  @JsonProperty("keyParts")
  private ArrayList<Object> keyParts;
  private int hashCode;

  @JsonCreator
  public SamzaSqlCompositeKey(@JsonProperty("keyParts") List<Object> keyParts) {
    this.keyParts = new ArrayList<>(keyParts);
    hashCode = keyParts.hashCode();
  }

  /**
   * Get the keyParts of all the columns in the relational message.
   * @return the keyParts of all the columns
   */
  @JsonProperty("keyParts")
  public ArrayList<Object> getKeyParts() {
    return keyParts;
  }

  @Override
  public String toString() {
    return Arrays.toString(keyParts.toArray());
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o != null && getClass() == o.getClass() && keyParts.equals(((SamzaSqlCompositeKey) o).keyParts);
  }

  /**
   * Create the SamzaSqlCompositeKey from the rel message.
   * @param message Represents the samza sql rel message.
   * @param relIdx list of keys in the form of field indices within the rel message.
   */
  public static SamzaSqlCompositeKey createSamzaSqlCompositeKey(SamzaSqlRelMessage message, List<Integer> relIdx) {
    ArrayList<Object> keyParts = new ArrayList<>();
    for (int idx : relIdx) {
      keyParts.add(message.getFieldValues().get(idx));
    }
    return new SamzaSqlCompositeKey(keyParts);
  }
}
