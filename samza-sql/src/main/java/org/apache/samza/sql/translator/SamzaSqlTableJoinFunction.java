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

package org.apache.samza.sql.translator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.lang3.Validate;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base Join Class between A Stream and A Lookup based Table using the Nested Loop join Algorithm.
 * For each incoming {@link SamzaSqlRelMessage} from the Stream DO a LOOKUP on the Table.
 *
 * There is 3 key steps:
 * 1 - Extract the Join Key from the incoming Message Based on the Join Predicate see {@link SamzaSqlTableJoinFunction#getMessageKeyRelRecord(org.apache.samza.sql.data.SamzaSqlRelMessage)}
 * 2 - Convert the Join Key to the Table Key Format and Execute the Lookup (Delegated to underline table implementation) {@link StreamTableJoinFunction#getMessageKey(java.lang.Object)}.
 * 3 - Compute the Join Result by combining the input Record from Stream and Result from Table Lookup {@link SamzaSqlTableJoinFunction#apply(org.apache.samza.sql.data.SamzaSqlRelMessage, Object)}.
 */
public abstract class SamzaSqlTableJoinFunction<K, R>
    implements StreamTableJoinFunction<K, SamzaSqlRelMessage, R, SamzaSqlRelMessage> {

  private static final Logger log = LoggerFactory.getLogger(SamzaSqlTableJoinFunction.class);

  private final JoinRelType joinRelType;
  private final boolean isTablePosOnRight;
  private final ArrayList<Integer> streamFieldIds;
  private final ArrayList<Integer> tableKeyIds;
  // Table field names are used in the outer join when the table record is not found.
  private final ArrayList<String> tableFieldNames;
  private final ArrayList<String> outFieldNames;
  final private List<Object> nullRow;

  SamzaSqlTableJoinFunction(JoinInputNode streamNode, JoinInputNode tableNode, JoinRelType joinRelType) {
    this.joinRelType = joinRelType;
    this.isTablePosOnRight = tableNode.isPosOnRight();

    Validate.isTrue((joinRelType.compareTo(JoinRelType.LEFT) == 0 && isTablePosOnRight) ||
        (joinRelType.compareTo(JoinRelType.RIGHT) == 0 && !isTablePosOnRight) ||
        joinRelType.compareTo(JoinRelType.INNER) == 0);

    this.streamFieldIds = new ArrayList<>(streamNode.getKeyIds());
    this.tableKeyIds = new ArrayList<>(tableNode.getKeyIds());
    this.tableFieldNames = new ArrayList<>(tableNode.getFieldNames());

    this.outFieldNames = new ArrayList<>();
    if (isTablePosOnRight) {
      outFieldNames.addAll(streamNode.getFieldNames());
      outFieldNames.addAll(tableFieldNames);
    } else {
      outFieldNames.addAll(tableFieldNames);
      outFieldNames.addAll(streamNode.getFieldNames());
    }
    nullRow = tableFieldNames.stream().map(x -> null).collect(Collectors.toList());
  }

  @Override
  public SamzaSqlRelMessage apply(SamzaSqlRelMessage message, R record) {

    if (joinRelType.compareTo(JoinRelType.INNER) == 0 && record == null) {
      log.debug("Inner Join: Record not found for the message with key: " + getMessageKey(message));
      // Returning null would result in Join operator implementation to filter out the message.
      return null;
    }

    // The resulting join output should be a SamzaSqlRelMessage containing the fields from both the stream message and
    // table record. The order of stream message fields and table record fields are dictated by the position of stream
    // and table in the 'from' clause of sql query. The output should also include the keys from both the stream message
    // and the table record.
    List<Object> outFieldValues = new ArrayList<>();

    // If table position is on the right, add the stream message fields first
    if (isTablePosOnRight) {
      outFieldValues.addAll(message.getSamzaSqlRelRecord().getFieldValues());
    }

    // Add the table record fields.
    if (record != null) {
      List<Object> row = getTableRelRecordFieldValues(record);
      // null in case the filter did not match thus row has to be removed if inner join or padded null case outer join
      if (row == null && joinRelType.compareTo(JoinRelType.INNER) == 0) return null;
      outFieldValues.addAll(row == null ? nullRow : row);
    } else {
      // Table record could be null as the record could not be found in the store. This can
      // happen for outer joins. Add nulls to all the field values in the output message.
      tableFieldNames.forEach(s -> outFieldValues.add(null));
    }

    // If table position is on the left, add the stream message fields last
    if (!isTablePosOnRight) {
      outFieldValues.addAll(message.getSamzaSqlRelRecord().getFieldValues());
    }

    return new SamzaSqlRelMessage(outFieldNames, outFieldValues, message.getSamzaSqlRelMsgMetadata());
  }

  /**
   * Map the resulting Record from the Table Side to a Primitive Java Type.
   * @param record join result to be converted
   * @return ordered List of columns as expected by the Join operator.
   */
  protected abstract List<Object> getTableRelRecordFieldValues(R record);

  /**
   * Computes the Rel Record out of the Join Predicate. Today join predicate is conjunction of equalities thus,
   * the record is a project of Values from Stream side with column names from the Table.
   * @param message input record from the Stream side.
   * @return join lookup key used to execute the inner lookup against Table.
   */
  protected SamzaSqlRelRecord getMessageKeyRelRecord(SamzaSqlRelMessage message) {
    // Extract the column names as they appear in the Table Side.
    final List<String> fieldNames = getSamzaSqlCompositeKeyFieldNames(tableFieldNames, tableKeyIds);
    // Extract the values used by the join predicate and compose it with Names from Table
    // NOTE that when join predicate has more that one clause there is an implicit contract:
    // The ORDER of field's name and values MATTER, this concern Local table joins only.
    // Remote table supports only one equality over the table primary key.
    return createSamzaSqlCompositeKey(message, streamFieldIds, fieldNames);
  }

  @Override
  public void close() {
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
