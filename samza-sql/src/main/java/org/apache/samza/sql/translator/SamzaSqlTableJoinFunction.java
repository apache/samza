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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.lang3.Validate;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.data.SamzaSqlRelMessage.createSamzaSqlCompositeKey;
import static org.apache.samza.sql.data.SamzaSqlRelMessage.getSamzaSqlCompositeKeyFieldNames;


/**
 * This abstract class joins incoming {@link SamzaSqlRelMessage} with records from a table with the join key.
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
      outFieldValues.addAll(getTableRelRecordFieldValues(record));
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

  protected abstract List<Object> getTableRelRecordFieldValues(R record);

  protected SamzaSqlRelRecord getMessageKeyRelRecord(SamzaSqlRelMessage message) {
    return getMessageKeyRelRecord(message, streamFieldIds, tableFieldNames, tableKeyIds);
  }

  public static SamzaSqlRelRecord getMessageKeyRelRecord(SamzaSqlRelMessage message, List<Integer> streamFieldIds,
      List<String> tableFieldNames, List<Integer> tableKeyIds) {
    return createSamzaSqlCompositeKey(message, streamFieldIds,
        getSamzaSqlCompositeKeyFieldNames(tableFieldNames, tableKeyIds));
  }

  @Override
  public void close() {
  }
}
