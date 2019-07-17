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

package org.apache.samza.sql;

import com.google.common.base.Joiner;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.samza.annotation.InterfaceStability;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Samza sql relational record. A record consists of list of column values and the associated column names.
 * A column value could be nested, meaning, it could be another SamzaSqlRelRecord.
 * Right now we do not store any metadata (like nullability, etc) other than the column name in the SamzaSqlRelRecord.
 */
@InterfaceStability.Unstable
public class SamzaSqlRelRecord implements Serializable {

  @JsonProperty("fieldNames")
  private final ArrayList<String> fieldNames;
  @JsonProperty("fieldValues")
  private final ArrayList<Object> fieldValues;
  private final int hashCode;

  /**
   * Creates a {@link SamzaSqlRelRecord} from the list of relational fields and values.
   * @param fieldNames Ordered list of field names in the row.
   * @param fieldValues  Ordered list of all the values in the row. Some of the fields can be null. This could be
   *                     result of delete change capture event in the stream or because of the result of the outer
   *                     join or the fields themselves are null in the original stream.
   */
  public SamzaSqlRelRecord(@JsonProperty("fieldNames") List<String> fieldNames,
      @JsonProperty("fieldValues") List<Object> fieldValues) {
    if (fieldNames.size() != fieldValues.size()) {
      throw new IllegalArgumentException("Field Names and values are not of same length.");
    }

    this.fieldNames = new ArrayList<>();
    this.fieldValues = new ArrayList<>();

    this.fieldNames.addAll(fieldNames);
    this.fieldValues.addAll(fieldValues);

    hashCode = Objects.hash(fieldNames, fieldValues);
  }

  /**
   * Get the field names of all the columns in the relational message.
   * @return the field names of all columns.
   */
  @JsonProperty("fieldNames")
  public List<String> getFieldNames() {
    return this.fieldNames;
  }

  /**
   * Get the field values of all the columns in the relational message.
   * @return the field values of all columns.
   */
  @JsonProperty("fieldValues")
  public List<Object> getFieldValues() {
    return this.fieldValues;
  }

  /**
   * Get the value of the field corresponding to the field name.
   * @param name Name of the field.
   * @return returns the value of the field.
   */
  public Optional<Object> getField(String name) {
    int index = fieldNames.indexOf(name);
    if (index != -1) {
      return Optional.ofNullable(fieldValues.get(index));
    }

    return Optional.empty();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SamzaSqlRelRecord other = (SamzaSqlRelRecord) obj;
    return Objects.equals(fieldNames, other.fieldNames) && Objects.equals(fieldValues, other.fieldValues);
  }

  @Override
  public String toString() {
    String nameStr = Joiner.on(",").join(fieldNames);
    String valueStr = Joiner.on(",").useForNull("null").join(fieldValues);
    return "[Names:{" + nameStr + "} Values:{" + valueStr + "}]";
  }

  public boolean containsField(String name) {
    return fieldNames.indexOf(name) != -1;
  }
}
