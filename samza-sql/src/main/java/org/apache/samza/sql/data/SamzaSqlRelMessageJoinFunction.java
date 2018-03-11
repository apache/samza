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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.lang.Validate;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.data.SamzaSqlCompositeKey.*;


public class SamzaSqlRelMessageJoinFunction implements StreamTableJoinFunction<SamzaSqlCompositeKey,
  SamzaSqlRelMessage, KV<SamzaSqlCompositeKey, SamzaSqlRelMessage>, SamzaSqlRelMessage> {

  private static final Logger log = LoggerFactory.getLogger(SamzaSqlRelMessageJoinFunction.class);

  private JoinRelType joinRelType;
  private boolean isTablePosOnRight;
  private List<Integer> streamFieldIds;
  // Table field names are used in the outer join when the table record is not found.
  private List<String> tableFieldNames;

  public SamzaSqlRelMessageJoinFunction(JoinRelType joinRelType, boolean isTablePosOnRight,
      List<Integer> streamFieldIds, List<String> tableFieldNames) {
    this.joinRelType = joinRelType;
    this.isTablePosOnRight = isTablePosOnRight;
    Validate.isTrue((joinRelType.compareTo(JoinRelType.LEFT) == 0 && isTablePosOnRight) ||
        (joinRelType.compareTo(JoinRelType.RIGHT) == 0 && !isTablePosOnRight) ||
        joinRelType.compareTo(JoinRelType.INNER) == 0);
    this.streamFieldIds = streamFieldIds;
    this.tableFieldNames = tableFieldNames;
  }

  @Override
  public SamzaSqlRelMessage apply(SamzaSqlRelMessage message, KV<SamzaSqlCompositeKey, SamzaSqlRelMessage> record) {

    if (joinRelType.compareTo(JoinRelType.INNER) == 0 && record == null) {
      log.debug("Record not found for the message with key: " + getMessageKey(message));
      return null;
    }

    // The resulting join output should be a SamzaSqlRelMessage containing the fields from both the stream message and
    // table record. The order of stream message fields and table record fields are dictated by the sql query. The
    // output should also include the keys from both the stream message and the table record.
    List<String> outFieldNames = new ArrayList<>();
    List<Object> outFieldValues = new ArrayList<>();

    // If table position is on the right, add the stream message fields first
    if (isTablePosOnRight) {
      outFieldNames.addAll(message.getFieldNames());
      outFieldValues.addAll(message.getFieldValues());
    }

    // Add the table record fields.
    if (record != null) {
      outFieldNames.addAll(record.getValue().getFieldNames());
      outFieldValues.addAll(record.getValue().getFieldValues());
    } else {
      // Table record could be null as the record could not be found in the store. This can
      // happen for outer joins. Add nulls to all the field values in the output message.
      outFieldNames.addAll(tableFieldNames);
      tableFieldNames.forEach(s -> outFieldValues.add(null));
    }

    // If table position is on the left, add the stream message fields last
    if (!isTablePosOnRight) {
      outFieldNames.addAll(message.getFieldNames());
      outFieldValues.addAll(message.getFieldValues());
    }

    return new SamzaSqlRelMessage(outFieldNames, outFieldValues);
  }

  @Override
  public SamzaSqlCompositeKey getMessageKey(SamzaSqlRelMessage message) {
    //return (SamzaSqlCompositeKey) message.getKey();
    return createSamzaSqlCompositeKey(message, streamFieldIds);
  }

  @Override
  public SamzaSqlCompositeKey getRecordKey(KV<SamzaSqlCompositeKey, SamzaSqlRelMessage> record) {
    return record.getKey();
  }

  @Override
  public void close() {
  }
}
