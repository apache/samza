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
 * of column values and the associated column names. Please note that the primary key itself could be a
 * {@link SamzaSqlRelRecord}.
 */
public class SamzaSqlRelMessage implements Serializable {

  public static final String KEY_NAME = "__key__";

  public static final String OP_NAME = "__op__";

  public static final String DELETE_OP = "DELETE";

  // key could be a record in itself.
  private final Object key;

  @JsonProperty("samzaSqlRelRecord")
  private final SamzaSqlRelRecord samzaSqlRelRecord;

  /**
   * hold metadata about the message or event, e.g., the eventTime timestamp
   */
  @JsonProperty("samzaSqlRelMsgMetadata")
  private SamzaSqlRelMsgMetadata samzaSqlRelMsgMetadata;

  /**
   * Creates a {@link SamzaSqlRelMessage} from the list of relational fields and values.
   * If the field list contains KEY, then it extracts the key out of the fields to create a
   * {@link SamzaSqlRelRecord} along with key, otherwise creates a {@link SamzaSqlRelRecord}
   * without the key.
   * @param fieldNames Ordered list of field names in the row.
   * @param fieldValues  Ordered list of all the values in the row. Some of the fields can be null, This could be
   *                     result of delete change capture event in the stream or because of the result of the outer join
   *                     or the fields themselves are null in the original stream.
   * @param metadata the message/event's metadata
   */
  public SamzaSqlRelMessage(List<String> fieldNames, List<Object> fieldValues, SamzaSqlRelMsgMetadata metadata) {
    Validate.isTrue(fieldNames.size() == fieldValues.size(), "Field Names and values are not of same length.");
    Validate.notNull(metadata, "Message metadata is NULL");

    int keyIndex = fieldNames.indexOf(KEY_NAME);
    Object key = null;
    if (keyIndex != -1) {
      key = fieldValues.get(keyIndex);
    }
    this.key = key;
    this.samzaSqlRelRecord = new SamzaSqlRelRecord(fieldNames, fieldValues);
    this.samzaSqlRelMsgMetadata = metadata;
  }


  /**
   * Create the SamzaSqlRelMessage, Each rel message represents a row in the table.
   * So it can contain a key and a list of fields in the row.
   * @param key Represents the key in the row, Key can be null.
   * @param fieldNames Ordered list of field names in the row.
   * @param fieldValues Ordered list of all the values in the row. Some of the fields can be null, This could be result of
   *               delete change capture event in the stream or because of the result of the outer join or the fields
   *               themselves are null in the original stream.
   * @param metadata the message/event's metadata
   */
  public SamzaSqlRelMessage(Object key, List<String> fieldNames, List<Object> fieldValues, SamzaSqlRelMsgMetadata metadata) {
    Validate.isTrue(fieldNames.size() == fieldValues.size(), "Field Names and values are not of same length.");
    Validate.notNull(metadata, "Message metadata is NULL");

    List<String> tmpFieldNames = new ArrayList<>();
    List<Object> tmpFieldValues = new ArrayList<>();

    this.key = key;
    tmpFieldNames.add(KEY_NAME);
    tmpFieldValues.add(key);

    tmpFieldNames.addAll(fieldNames);
    tmpFieldValues.addAll(fieldValues);

    this.samzaSqlRelRecord = new SamzaSqlRelRecord(tmpFieldNames, tmpFieldValues);
    this.samzaSqlRelMsgMetadata = metadata;
  }

  /**
   * Creates the SamzaSqlRelMessage from {@link SamzaSqlRelRecord}.
   * @param samzaSqlRelRecord represents the rel record.
   * @param metadata the message/event's metadata
   */
  public SamzaSqlRelMessage(@JsonProperty("samzaSqlRelRecord") SamzaSqlRelRecord samzaSqlRelRecord,
      @JsonProperty("samzaSqlRelMsgMetadata") SamzaSqlRelMsgMetadata metadata) {
    this(samzaSqlRelRecord.getFieldNames(), samzaSqlRelRecord.getFieldValues(), metadata);
  }

  @JsonProperty("samzaSqlRelRecord")
  public SamzaSqlRelRecord getSamzaSqlRelRecord() {
    return samzaSqlRelRecord;
  }

  @JsonProperty("samzaSqlRelMsgMetadata")
  public SamzaSqlRelMsgMetadata getSamzaSqlRelMsgMetadata() { return samzaSqlRelMsgMetadata; }

  public Object getKey() {
    return key;
  }

  public void setEventTime(String eventTime) {
    this.samzaSqlRelMsgMetadata.setEventTime(eventTime);
  }

  public void setArrivalTime(String arrivalTime) {
    this.samzaSqlRelMsgMetadata.setArrivalTime(arrivalTime);
  }

  public void setScanTime(String scanTime) {
    this.samzaSqlRelMsgMetadata.setScanTime(scanTime);
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

  @Override
  public String toString() {
    return "RelMessage: {" + samzaSqlRelRecord + " " + samzaSqlRelMsgMetadata + "}";
  }

  /**
   * Create composite key from the rel message.
   * @param message Represents the samza sql rel message to extract the key values from.
   * @param keyValueIdx list of key values in the form of field indices within the rel message.
   * @param keyPartNames Represents the key field names.
   * @return the composite key of the rel message
   */
  public static SamzaSqlRelRecord createSamzaSqlCompositeKey(SamzaSqlRelMessage message, List<Integer> keyValueIdx,
      List<String> keyPartNames) {
    Validate.isTrue(keyValueIdx.size() == keyPartNames.size(), "Key part name and value list sizes are different");
    ArrayList<Object> keyPartValues = new ArrayList<>();
    for (int idx : keyValueIdx) {
      keyPartValues.add(message.getSamzaSqlRelRecord().getFieldValues().get(idx));
    }
    return new SamzaSqlRelRecord(keyPartNames, keyPartValues);
  }

  /**
   * Create composite key from the rel message.
   * @param message Represents the samza sql rel message to extract the key values and names from.
   * @param relIdx list of keys in the form of field indices within the rel message.
   * @return the composite key of the rel message
   */
  public static SamzaSqlRelRecord createSamzaSqlCompositeKey(SamzaSqlRelMessage message, List<Integer> relIdx) {
    return createSamzaSqlCompositeKey(message, relIdx,
        getSamzaSqlCompositeKeyFieldNames(message.getSamzaSqlRelRecord().getFieldNames(), relIdx));
  }

  /**
   * Get composite key field names.
   * @param fieldNames list of field names to extract the key names from.
   * @param nameIds indices within the field names.
   * @return list of composite key field names
   */
  public static List<String> getSamzaSqlCompositeKeyFieldNames(List<String> fieldNames,
      List<Integer> nameIds) {
    List<String> keyPartNames = new ArrayList<>();
    for (int idx : nameIds) {
      keyPartNames.add(fieldNames.get(idx));
    }
    return keyPartNames;
  }

}