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
package org.apache.samza.operators.spec;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.operators.impl.store.TimestampedValueSerde;
import org.apache.samza.operators.impl.store.TimestampedValue;
import org.apache.samza.serializers.Serde;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;


/**
 * The spec for the join operator that buffers messages from one stream and
 * joins them with buffered messages from another stream.
 *
 * @param <K>  the type of join key
 * @param <M>  the type of message in this stream
 * @param <OM>  the type of message in the other stream
 * @param <JM>  the type of join result
 */
public class JoinOperatorSpec<K, M, OM, JM> extends OperatorSpec<Object, JM> implements StatefulOperatorSpec { // Object == M | OM

  private final JoinFunction<K, M, OM, JM> joinFn;
  private final long ttlMs;

  private final OperatorSpec<?, M> leftInputOpSpec;
  private final OperatorSpec<?, OM> rightInputOpSpec;

  /**
   * The following {@link Serde}s are serialized by the ExecutionPlanner when generating the store configs for a join, and
   * deserialized once during startup in SamzaContainer. They don't need to be deserialized here on a per-task basis
   */
  private transient final Serde<K> keySerde;
  private transient final Serde<TimestampedValue<M>> messageSerde;
  private transient final Serde<TimestampedValue<OM>> otherMessageSerde;

  /**
   * Default constructor for a {@link JoinOperatorSpec}.
   *
   * @param leftInputOpSpec  the operator spec for the stream on the left side of the join
   * @param rightInputOpSpec  the operator spec for the stream on the right side of the join
   * @param joinFn  the user-defined join function to get join keys and results
   * @param ttlMs  the ttl in ms for retaining messages in each stream
   * @param opId  the unique ID for this operator
   */
  JoinOperatorSpec(OperatorSpec<?, M> leftInputOpSpec, OperatorSpec<?, OM> rightInputOpSpec,
      JoinFunction<K, M, OM, JM> joinFn, Serde<K> keySerde, Serde<M> messageSerde, Serde<OM> otherMessageSerde,
      long ttlMs, String opId) {
    super(OpCode.JOIN, opId);
    this.leftInputOpSpec = leftInputOpSpec;
    this.rightInputOpSpec = rightInputOpSpec;
    this.joinFn = joinFn;
    this.keySerde = keySerde;
    this.messageSerde = new TimestampedValueSerde<>(messageSerde);
    this.otherMessageSerde = new TimestampedValueSerde<>(otherMessageSerde);
    this.ttlMs = ttlMs;
  }

  @Override
  public Collection<StoreDescriptor> getStoreDescriptors() {
    String rocksDBStoreFactory = "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory";
    String leftStoreName = getLeftOpId();
    String rightStoreName = getRightOpId();
    Map<String, String> leftStoreCustomProps = ImmutableMap.of(
        String.format("stores.%s.rocksdb.ttl.ms", leftStoreName), Long.toString(ttlMs),
        String.format("stores.%s.changelog.kafka.cleanup.policy", leftStoreName), "delete",
        String.format("stores.%s.changelog.kafka.retention.ms", leftStoreName), Long.toString(ttlMs));
    Map<String, String> rightStoreCustomProps = ImmutableMap.of(
        String.format("stores.%s.rocksdb.ttl.ms", rightStoreName), Long.toString(ttlMs),
        String.format("stores.%s.changelog.kafka.cleanup.policy", rightStoreName), "delete",
        String.format("stores.%s.changelog.kafka.retention.ms", rightStoreName), Long.toString(ttlMs));

    return Arrays.asList(
        new StoreDescriptor(leftStoreName, rocksDBStoreFactory, this.keySerde, this.messageSerde,
            leftStoreName, leftStoreCustomProps),
        new StoreDescriptor(rightStoreName, rocksDBStoreFactory, this.keySerde, this.otherMessageSerde,
            rightStoreName, rightStoreCustomProps));
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return joinFn instanceof WatermarkFunction ? (WatermarkFunction) joinFn : null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return joinFn instanceof TimerFunction ? (TimerFunction) joinFn : null;
  }

  public OperatorSpec getLeftInputOpSpec() {
    return leftInputOpSpec;
  }

  public OperatorSpec getRightInputOpSpec() {
    return rightInputOpSpec;
  }

  public String getLeftOpId() {
    return this.getOpId() + "-L";
  }

  public String getRightOpId() {
    return this.getOpId() + "-R";
  }

  public JoinFunction<K, M, OM, JM> getJoinFn() {
    return this.joinFn;
  }

  public long getTtlMs() {
    return ttlMs;
  }

}
