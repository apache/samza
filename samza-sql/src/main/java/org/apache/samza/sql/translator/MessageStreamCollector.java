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

import java.io.Closeable;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.AsyncFlatMapFunction;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.apache.samza.table.Table;


/**
 * Collector of Map and Filter Samza Functions to collect call stack on the top of Remote table.
 * This Collector will be used by Join operator and trigger it when applying the join function post lookup.
 *
 * Note that this is needed because the Remote Table can not expose a proper {@code MessageStream}.
 * It is a work around to minimize the amount of code changes of the current Query Translator {@link org.apache.samza.sql.translator.QueryTranslator},
 * But in an ideal world, we should use Calcite planner in conventional way we can combine function when via translation of RelNodes.
 */
class MessageStreamCollector implements MessageStream<SamzaSqlRelMessage>, Serializable, Closeable {

  /**
   * Queue First in First to be Fired order of the operators on the top of Remote Table Scan.
   */
  private final Deque<MapFunction<? super SamzaSqlRelMessage, ? extends SamzaSqlRelMessage>> mapFnCallQueue =
      new ArrayDeque<>();
  /**
   * Table Scan Rel Node Id.
   */
  private final int tableScanId;
  /**
   * SQL statement Id.
   */
  private final int queryId;

  /**
   * Function to chain the call to close from each operator.
   */
  private transient Function<Void, Void> closeFn = aVoid -> null;

  MessageStreamCollector(TableScan tableScan, int queryId) {
    this.tableScanId = tableScan.getId();
    this.queryId = queryId;
  }

  @Override
  public <OM> MessageStream<OM> map(MapFunction<? super SamzaSqlRelMessage, ? extends OM> mapFn) {
    mapFnCallQueue.offer((MapFunction<? super SamzaSqlRelMessage, ? extends SamzaSqlRelMessage>) mapFn);
    return (MessageStream<OM>) this;
  }

  @Override
  public MessageStream<SamzaSqlRelMessage> filter(FilterFunction<? super SamzaSqlRelMessage> filterFn) {
    mapFnCallQueue.offer(new FilterMapAdapter(filterFn));
    return this;
  }

  /**
   * This function is called by the join operator on run time to apply filter and projects post join lookup.
   *
   * @param context Samza Execution Context
   * @return {code null} case filter reject the row, Samza Relational Record as it goes via Projects.
   */
  Function<SamzaSqlRelMessage, SamzaSqlRelMessage> getFunction(Context context) {
    TranslatorContext translatorContext =
        ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
    RelNode rel = translatorContext.getRelNode(tableScanId);
    assert rel instanceof TableScan;
    // First Map Function to Ensure that incoming rel message field positions in synch with the Schema of Table Scan
    final List<String> fieldNames = rel.getRowType().getFieldNames();
    Function<SamzaSqlRelMessage, SamzaSqlRelMessage> tailFn =  new ScanTranslator.ReArrangeFn(fieldNames);
    Function<Void, Void> intFn = aVoid -> null; // Projects and Filters both need to be initialized.
    closeFn = aVoid -> null;
    // At this point we have a the queue of operator, where first in is the first operator on top of TableScan.
    while (!mapFnCallQueue.isEmpty()) {
      MapFunction<? super SamzaSqlRelMessage, ? extends SamzaSqlRelMessage> f = mapFnCallQueue.poll();
      intFn = intFn.andThen((aVoid) -> {
        f.init(context);
        return null;
      });
      closeFn.andThen((aVoid) -> {
        f.close();
        return null;
      });

      Function<SamzaSqlRelMessage, SamzaSqlRelMessage> current = x -> x == null ? null : f.apply(x);
      if (tailFn == null) {
        tailFn = current;
      } else {
        tailFn = current.compose(tailFn);
      }
    }
    intFn.apply(null);
    return tailFn;
  }

  /**
   * Filter adapter is used to compose filters with {@code MapFunction<SamzaSqlRelMessage, SamzaSqlRelMessage>}
   * Filter function will return {@code null} when input is {@code null} or filter condition reject current row.
   */
  private static class FilterMapAdapter implements MapFunction<SamzaSqlRelMessage, SamzaSqlRelMessage> {
    private final FilterFunction<? super SamzaSqlRelMessage> filterFn;

    private FilterMapAdapter(FilterFunction<? super SamzaSqlRelMessage> filterFn) {
      this.filterFn = filterFn;
    }

    @Override
    public SamzaSqlRelMessage apply(SamzaSqlRelMessage message) {
      if (message != null && filterFn.apply(message)) {
        return message;
      }
      // null on case no match
      return null;
    }

    @Override
    public void close() {
      filterFn.close();
    }

    @Override
    public void init(Context context) {
      filterFn.init(context);
    }
  }

  @Override
  public void close() {
    if (closeFn != null) {
      closeFn.apply(null);
    }
  }

  @Override
  public <OM> MessageStream<OM> flatMap(FlatMapFunction<? super SamzaSqlRelMessage, ? extends OM> flatMapFn) {
    return null;
  }

  @Override
  public <OM> MessageStream<OM> flatMapAsync(
      AsyncFlatMapFunction<? super SamzaSqlRelMessage, ? extends OM> asyncFlatMapFn) {
    return null;
  }

  @Override
  public void sink(SinkFunction<? super SamzaSqlRelMessage> sinkFn) {
    throw new IllegalStateException("Not valid state");
  }

  @Override
  public MessageStream<SamzaSqlRelMessage> sendTo(OutputStream<SamzaSqlRelMessage> outputStream) {
    throw new IllegalStateException("Not valid state");
  }

  @Override
  public <K, WV> MessageStream<WindowPane<K, WV>> window(Window<SamzaSqlRelMessage, K, WV> window, String id) {
    throw new IllegalStateException("Not valid state");
  }

  @Override
  public <K, OM, JM> MessageStream<JM> join(MessageStream<OM> otherStream,
      JoinFunction<? extends K, ? super SamzaSqlRelMessage, ? super OM, ? extends JM> joinFn, Serde<K> keySerde,
      Serde<SamzaSqlRelMessage> messageSerde, Serde<OM> otherMessageSerde, Duration ttl, String id) {
    throw new IllegalStateException("Not valid state");
  }

  @Override
  public <K, R extends KV, JM> MessageStream<JM> join(Table<R> table,
      StreamTableJoinFunction<? extends K, ? super SamzaSqlRelMessage, ? super R, ? extends JM> joinFn,
      Object... args) {
    throw new IllegalStateException("Not valid state");
  }

  @Override
  public MessageStream<SamzaSqlRelMessage> merge(
      Collection<? extends MessageStream<? extends SamzaSqlRelMessage>> otherStreams) {
    throw new IllegalStateException("Not valid state");
  }

  @Override
  public <K, V> MessageStream<KV<K, V>> partitionBy(MapFunction<? super SamzaSqlRelMessage, ? extends K> keyExtractor,
      MapFunction<? super SamzaSqlRelMessage, ? extends V> valueExtractor, KVSerde<K, V> serde, String id) {
    throw new IllegalStateException("Not valid state");
  }

  @Override
  public <K, V> MessageStream<KV<K, V>> sendTo(Table<KV<K, V>> table, Object... args) {
    throw new IllegalStateException("Not valid state");
  }

  @Override
  public MessageStream<SamzaSqlRelMessage> broadcast(Serde<SamzaSqlRelMessage> serde, String id) {
    throw new IllegalStateException("Not valid state");
  }
}
