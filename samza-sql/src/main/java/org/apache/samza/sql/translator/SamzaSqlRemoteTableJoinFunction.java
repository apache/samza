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
import java.util.Objects;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SamzaRelTableKeyConverter;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;


/**
 * This class joins incoming {@link SamzaSqlRelMessage} with records {@link KV} from a remote table with the join key
 * defined by the format of the table key.
 */
public class SamzaSqlRemoteTableJoinFunction
    extends SamzaSqlTableJoinFunction<Object, KV> {

  private transient SamzaRelConverter msgConverter;
  private transient SamzaRelTableKeyConverter relTableKeyConverter;
  private final String tableName;
  private final int queryId;

  SamzaSqlRemoteTableJoinFunction(SamzaRelConverter msgConverter, SamzaRelTableKeyConverter tableKeyConverter,
      JoinInputNode streamNode, JoinInputNode tableNode, JoinRelType joinRelType, int queryId) {
    super(streamNode, tableNode, joinRelType);

    this.msgConverter = msgConverter;
    this.relTableKeyConverter = tableKeyConverter;
    this.tableName = tableNode.getSourceName();
    this.queryId = queryId;
  }

  @Override
  public void init(Context context) {
    TranslatorContext translatorContext =
        ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
    this.msgConverter = translatorContext.getMsgConverter(tableName);
    this.relTableKeyConverter = translatorContext.getTableKeyConverter(tableName);
  }

  @Override
  protected List<Object> getTableRelRecordFieldValues(KV record) {
    // Using the message rel converter, convert message to sql rel message and add to output values.
    SamzaSqlRelMessage relMessage = msgConverter.convertToRelMessage(record);
    return relMessage.getSamzaSqlRelRecord().getFieldValues();
  }

  @Override
  public Object getMessageKey(SamzaSqlRelMessage message) {
    SamzaSqlRelRecord keyRecord = getMessageKeyRelRecord(message);
    // If all the message key rel record values are null, return null message key.
    if (keyRecord.getFieldValues().stream().allMatch(Objects::isNull)) {
      return null;
    }
    // Using the table key converter, convert message key from rel format to the record key format.
    return relTableKeyConverter.convertToTableKeyFormat(keyRecord);
  }

  @Override
  public Object getRecordKey(KV record) {
    return record.getKey();
  }
}
