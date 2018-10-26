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
import org.apache.commons.lang.Validate;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.data.SamzaSqlRelMessage.*;


/**
 * This class joins incoming {@link SamzaSqlRelMessage} from a stream with the records in a table with the join key
 * being {@link SamzaSqlRelRecord}
 */
public class SamzaSqlRelMessageJoinFunction
    implements StreamTableJoinFunction<SamzaSqlRelRecord, SamzaSqlRelMessage, KV<SamzaSqlRelRecord, SamzaSqlRelMessage>, SamzaSqlRelMessage> {

  private static final Logger log = LoggerFactory.getLogger(SamzaSqlRelMessageJoinFunction.class);

  private final JoinRelType joinRelType;
  private final boolean isTablePosOnRight;
  private final ArrayList<Integer> streamFieldIds;
  // Table field names are used in the outer join when the table record is not found.
  private final ArrayList<Integer> tableKeyIds;
  private final ArrayList<String> tableFieldNames;
  private final ArrayList<String> outFieldNames;

  SamzaSqlRelMessageJoinFunction(JoinRelType joinRelType, boolean isTablePosOnRight,
      List<Integer> streamFieldIds, List<String> streamFieldNames, List<Integer> tableKeyIds,
      List<String> tableFieldNames) {
    this.joinRelType = joinRelType;
    this.isTablePosOnRight = isTablePosOnRight;
    Validate.isTrue((joinRelType.compareTo(JoinRelType.LEFT) == 0 && isTablePosOnRight) ||
        (joinRelType.compareTo(JoinRelType.RIGHT) == 0 && !isTablePosOnRight) ||
        joinRelType.compareTo(JoinRelType.INNER) == 0);
    this.streamFieldIds = new ArrayList<>(streamFieldIds);
    this.tableKeyIds = new ArrayList<>(tableKeyIds);
    this.tableFieldNames = new ArrayList<>(tableFieldNames);
    this.outFieldNames = new ArrayList<>();
    if (isTablePosOnRight) {
      outFieldNames.addAll(streamFieldNames);
    }
    outFieldNames.addAll(tableFieldNames);
    if (!isTablePosOnRight) {
      outFieldNames.addAll(streamFieldNames);
    }
  }

  @Override
  public SamzaSqlRelMessage apply(SamzaSqlRelMessage message, KV<SamzaSqlRelRecord, SamzaSqlRelMessage> record) {

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
      outFieldValues.addAll(record.getValue().getSamzaSqlRelRecord().getFieldValues());
    } else {
      // Table record could be null as the record could not be found in the store. This can
      // happen for outer joins. Add nulls to all the field values in the output message.
      tableFieldNames.forEach(s -> outFieldValues.add(null));
    }

    // If table position is on the left, add the stream message fields last
    if (!isTablePosOnRight) {
      outFieldValues.addAll(message.getSamzaSqlRelRecord().getFieldValues());
    }

    return new SamzaSqlRelMessage(outFieldNames, outFieldValues);
  }

  @Override
  public SamzaSqlRelRecord getMessageKey(SamzaSqlRelMessage message) {
    return createSamzaSqlCompositeKey(message, streamFieldIds,
        getSamzaSqlCompositeKeyFieldNames(tableFieldNames, tableKeyIds));
  }

  @Override
  public SamzaSqlRelRecord getRecordKey(KV<SamzaSqlRelRecord, SamzaSqlRelMessage> record) {
    return record.getKey();
  }

  @Override
  public void close() {
  }
}
