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

import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;


/**
 * This class joins incoming {@link SamzaSqlRelMessage} with records {@link KV}&lt;{@link SamzaSqlRelRecord}, {@link SamzaSqlRelMessage}&gt;}
 * from local table with the join key being {@link SamzaSqlRelRecord}.
 */
public class SamzaSqlLocalTableJoinFunction
    extends SamzaSqlTableJoinFunction<SamzaSqlRelRecord, KV<SamzaSqlRelRecord, SamzaSqlRelMessage>> {

  SamzaSqlLocalTableJoinFunction(JoinInputNode streamNode, JoinInputNode tableNode, JoinRelType joinRelType) {
    super(streamNode, tableNode, joinRelType);
  }

  @Override
  protected List<Object> getTableRelRecordFieldValues(KV<SamzaSqlRelRecord, SamzaSqlRelMessage> record) {
    return record.getValue().getSamzaSqlRelRecord().getFieldValues();
  }

  @Override
  public SamzaSqlRelRecord getMessageKey(SamzaSqlRelMessage message) {
    return getMessageKeyRelRecord(message);
  }

  @Override
  public SamzaSqlRelRecord getRecordKey(KV<SamzaSqlRelRecord, SamzaSqlRelMessage> record) {
    return record.getKey();
  }
}
