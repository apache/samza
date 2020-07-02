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
import java.util.function.Function;
import javax.annotation.Nullable;
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
  /**
   * Projection and Filter Function to apply post the join lookup. Function will return null in case filter rejects row.
   */
  private Function<SamzaSqlRelMessage, SamzaSqlRelMessage> projectFunction;
  /**
   * Projects and Filters operator queue.
   */
  private final MessageStreamCollector messageStreamCollector;

  public SamzaSqlRemoteTableJoinFunction(SamzaRelConverter msgConverter, SamzaRelTableKeyConverter tableKeyConverter,
      JoinInputNode streamNode, JoinInputNode tableNode, JoinRelType joinRelType, int queryId,
      MessageStreamCollector messageStreamCollector) {
    super(streamNode, tableNode, joinRelType);
    this.msgConverter = msgConverter;
    this.relTableKeyConverter = tableKeyConverter;
    this.tableName = tableNode.getSourceName();
    this.queryId = queryId;
    this.messageStreamCollector = messageStreamCollector;
  }

  SamzaSqlRemoteTableJoinFunction(SamzaRelConverter msgConverter, SamzaRelTableKeyConverter tableKeyConverter,
      JoinInputNode streamNode, JoinInputNode tableNode, JoinRelType joinRelType, int queryId) {
    super(streamNode, tableNode, joinRelType);
    this.msgConverter = msgConverter;
    this.relTableKeyConverter = tableKeyConverter;
    this.tableName = tableNode.getSourceName();
    this.queryId = queryId;
    this.projectFunction = Function.identity();
    this.messageStreamCollector = null;
  }

  @Override
  public void init(Context context) {
    TranslatorContext translatorContext =
        ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
    this.msgConverter = translatorContext.getMsgConverter(tableName);
    this.relTableKeyConverter = translatorContext.getTableKeyConverter(tableName);
    if (messageStreamCollector != null) {
      projectFunction = messageStreamCollector.getFunction(context);
    }
  }

  /**
   * Compute the projection and filter post join lookup.
   *
   * @param record input record as result of lookup
   * @return the projected row or {@code null} if Row doesn't pass the filter condition.
   */
  @Override
  @Nullable
  protected List<Object> getTableRelRecordFieldValues(KV record) {
    // Using the message rel converter, convert message to sql rel message and add to output values.
    final SamzaSqlRelMessage relMessage = msgConverter.convertToRelMessage(record);
    final SamzaSqlRelMessage result = projectFunction.apply(relMessage);
    return result == null ? null : result.getSamzaSqlRelRecord().getFieldValues();
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

  @Override
  public void close() {
    super.close();
    if (messageStreamCollector != null) {
      messageStreamCollector.close();
    }
  }
}
