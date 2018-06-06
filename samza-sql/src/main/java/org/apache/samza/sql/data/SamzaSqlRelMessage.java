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
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.Validate;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Samza sql relational message. Each Samza sql relational message represents a relational row in a table.
 * Each row of the relational table consists of a primary key and {@link SamzaSqlRelRecord}, which consists of a list
 * of column values and the associated column names.
 */
public class SamzaSqlRelMessage implements Serializable {

  public static final String KEY_NAME = "__key__";

  private final Object key;

  @JsonProperty("samzaSqlRelRecord")
  private final SamzaSqlRelRecord samzaSqlRelRecord;

  /**
   * Creates a {@link SamzaSqlRelMessage} from the list of relational fields and values.
   * If the field list contains KEY, then it extracts the key out of the fields to create a
   * {@link SamzaSqlRelRecord} along with key, otherwise creates a {@link SamzaSqlRelRecord}
   * without the key.
   * @param fieldNames Ordered list of field names in the row.
   * @param fieldValues  Ordered list of all the values in the row. Some of the fields can be null, This could be
   *                     result of delete change capture event in the stream or because of the result of the outer join
   *                     or the fields themselves are null in the original stream.
   */
  public SamzaSqlRelMessage(List<String> fieldNames, List<Object> fieldValues) {
    Validate.isTrue(fieldNames.size() == fieldValues.size(), "Field Names and values are not of same length.");

    int keyIndex = fieldNames.indexOf(KEY_NAME);
    Object key = null;
    if (keyIndex != -1) {
      key = fieldValues.get(keyIndex);
    }
    this.key = key;
    this.samzaSqlRelRecord = new SamzaSqlRelRecord(fieldNames, fieldValues);
  }

  /**
   * Create the SamzaSqlRelMessage, Each rel message represents a row in the table.
   * So it can contain a key and a list of fields in the row.
   * @param key Represents the key in the row, Key can be null.
   * @param fieldNames Ordered list of field names in the row.
   * @param fieldValues Ordered list of all the values in the row. Some of the fields can be null, This could be result of
   *               delete change capture event in the stream or because of the result of the outer join or the fields
   *               themselves are null in the original stream.
   */
  public SamzaSqlRelMessage(Object key, List<String> fieldNames, List<Object> fieldValues) {
    Validate.isTrue(fieldNames.size() == fieldValues.size(), "Field Names and values are not of same length.");

    List<String> tmpFieldNames = new ArrayList<>();
    List<Object> tmpFieldValues = new ArrayList<>();

    this.key = key;
    tmpFieldNames.add(KEY_NAME);
    tmpFieldValues.add(key);

    tmpFieldNames.addAll(fieldNames);
    tmpFieldValues.addAll(fieldValues);

    this.samzaSqlRelRecord = new SamzaSqlRelRecord(tmpFieldNames, tmpFieldValues);
  }

  /**
   * Creates the SamzaSqlRelMessage from {@link SamzaSqlRelRecord}.
   * @param samzaSqlRelRecord represents the rel record.
   */
  public SamzaSqlRelMessage(@JsonProperty("samzaSqlRelRecord") SamzaSqlRelRecord samzaSqlRelRecord) {
    this(samzaSqlRelRecord.getFieldNames(), samzaSqlRelRecord.getFieldValues());
  }

  @JsonProperty("samzaSqlRelRecord")
  public SamzaSqlRelRecord getSamzaSqlRelRecord() {
    return samzaSqlRelRecord;
  }

  public Object getKey() {
    return key;
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, samzaSqlRelRecord);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SamzaSqlRelMessage other = (SamzaSqlRelMessage) obj;
    return Objects.equals(key, other.key) && Objects.equals(samzaSqlRelRecord, other.samzaSqlRelRecord);
  }
}
