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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang.Validate;


/**
 * Samza sql relational message. Each Samza sql relational message represents a relational row in a table.
 * Each row of the relational table and hence SamzaSqlRelMessage consists of list of column values and
 * their associated column names. Right now we donot store any other metadata other than the column name in the
 * SamzaSqlRelationalMessage, In future if we find a need, we could add additional column ddl metadata around
 * primary Key, nullability, etc.
 */
public class SamzaSqlRelMessage {

  public static final String KEY_NAME = "__key__";

  private final List<Object> value = new ArrayList<>();
  private final List<Object> relFieldValues = new ArrayList<>();
  private final List<String> names = new ArrayList<>();
  private final Object key;

  /**
   * Create the SamzaSqlRelMessage, Each rel message represents a row in the table.
   * So it can contain a key and a list of fields in the row.
   * @param key Represents the key in the row, Key is optional, in which case it can be null.
   * @param names Ordered list of field names in the row.
   * @param values Ordered list of all the values in the row. Since the samzaSqlRelMessage can represent
   *               the row in a change capture event stream, It can contain delete messages in which case
   *               all the fields in the row can be null.
   */
  public SamzaSqlRelMessage(Object key, List<String> names, List<Object> values) {
    Validate.isTrue(names.size() == values.size(), "Field Names and values are not of same length.");
    this.key = key;
    this.value.addAll(values);
    this.names.addAll(names);
    if (key != null) {
      this.relFieldValues.add(key);
    }
    this.relFieldValues.addAll(values);
  }

  /**
   * Get the field names of all the columns in the relational message.
   * @return the field names of all columns.
   */
  public List<String> getFieldNames() {
    return names;
  }

  /**
   * Get the values of all the columns in the relational message.
   * @return the values of all the columns
   */
  public List<Object> getFieldValues() {
    return value;
  }

  public List<Object> getRelFieldValues() {
    return this.relFieldValues;
  }

  public Object getKey() {
    return key;
  }

  /**
   * Get the value of the field corresponding to the field name.
   * @param name Name of the field.
   * @return returns the value of the field.
   */
  public Optional<Object> getField(String name) {
    for (int index = 0; index < names.size(); index++) {
      if (names.get(index).equals(name)) {
        return Optional.ofNullable(value.get(index));
      }
    }

    return Optional.empty();
  }

  /**
   * Creates a {@link SamzaSqlRelMessage} from the list of relational fields and values.
   * If the field list contains KEY, then it extracts the key out of the fields to create the
   * RelMessage with key and values.
   * @param fieldValues Field values that can include the key as well.
   * @param fieldNames Field names in the rel message that can include the special __key__
   * @return Created SamzaSqlRelMessage.
   */
  public static SamzaSqlRelMessage createRelMessage(List<Object> fieldValues, List<String> fieldNames) {
    int keyIndex = fieldNames.indexOf(KEY_NAME);
    fieldNames = new ArrayList<>(fieldNames);
    fieldValues = new ArrayList<>(fieldValues);
    Object key = null;
    if (keyIndex != -1) {
      key = fieldValues.get(keyIndex);
      fieldValues.remove(keyIndex);
      fieldNames.remove(keyIndex);
    }

    return new SamzaSqlRelMessage(key, fieldNames, fieldValues);
  }
}
