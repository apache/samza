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

package org.apache.samza.execution;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.SendToTableOperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.table.TableSpec;


/**
 * A utility class that encapsulates the logic for traversing operators in the graph from the set of {@link InputOperatorSpec}
 * and building associations between related {@link OperatorSpec}s.
 */
/* package private */ class OperatorSpecGraphAnalyzer {

  /**
   * Returns a grouping of {@link InputOperatorSpec}s by the joins, i.e. {@link JoinOperatorSpec}s and
   * {@link StreamTableJoinOperatorSpec}s, they participate in.
   *
   * The key of the returned Multimap is of type {@link OperatorSpec} due to the lack of a stricter
   * base type for {@link JoinOperatorSpec} and {@link StreamTableJoinOperatorSpec}. However, key
   * objects are guaranteed to be of either type only.
   */
  public static Multimap<OperatorSpec, InputOperatorSpec> getJoinToInputOperatorSpecs(
      Collection<InputOperatorSpec> inputOpSpecs) {

    Multimap<OperatorSpec, InputOperatorSpec> joinToInputOpSpecs = HashMultimap.create();

    // Create a getNextOpSpecs() function that emulates connections between every SendToTableOperatorSpec
    // — which are terminal OperatorSpecs — and all StreamTableJoinOperatorSpecs referencing the same TableSpec.
    //
    // This is necessary to support Stream-Table Join scenarios because it allows us to associate streams behind
    // SendToTableOperatorSpecs with streams participating in Stream-Table Joins, a connection that would not be
    // easy to make otherwise since SendToTableOperatorSpecs are terminal operator specs.
    Function<OperatorSpec, Iterable<OperatorSpec>> getNextOpSpecs = getCustomGetNextOpSpecs(inputOpSpecs);

    // Traverse graph starting from every input operator spec, observing connectivity between input operator specs
    // and join-related operator specs.
    for (InputOperatorSpec inputOpSpec : inputOpSpecs) {
      // Observe all join-related operator specs reachable from this input operator spec.
      JoinVisitor joinVisitor = new JoinVisitor();
      traverse(inputOpSpec, joinVisitor, getNextOpSpecs);

      // Associate every encountered join-related operator spec with this input operator spec.
      for (OperatorSpec joinOpSpec : joinVisitor.getJoins()) {
        joinToInputOpSpecs.put(joinOpSpec, inputOpSpec);
      }
    }

    return joinToInputOpSpecs;
  }

  /**
   * Traverses {@link OperatorSpec}s starting from {@code startOpSpec}, invoking {@code visitor} with every encountered
   * {@link OperatorSpec}, and using {@code getNextOpSpecs} to determine the set of {@link OperatorSpec}s to visit next.
   */
  private static void traverse(OperatorSpec startOpSpec, Consumer<OperatorSpec> visitor,
      Function<OperatorSpec, Iterable<OperatorSpec>> getNextOpSpecs) {
    visitor.accept(startOpSpec);
    for (OperatorSpec nextOpSpec : getNextOpSpecs.apply(startOpSpec)) {
      traverse(nextOpSpec, visitor, getNextOpSpecs);
    }
  }

  /**
   * Creates a function that retrieves the next {@link OperatorSpec}s of any given {@link OperatorSpec} in the specified
   * {@code operatorSpecGraph}.
   *
   * Calling the returned function with any {@link SendToTableOperatorSpec} will return a collection of all
   * {@link StreamTableJoinOperatorSpec}s that reference the same {@link TableSpec} as the specified
   * {@link SendToTableOperatorSpec}, as if they were actually connected.
   */
  private static Function<OperatorSpec, Iterable<OperatorSpec>> getCustomGetNextOpSpecs(
      Iterable<InputOperatorSpec> inputOpSpecs) {

    // Traverse operatorSpecGraph to create mapping between every SendToTableOperatorSpec and all
    // StreamTableJoinOperatorSpecs referencing the same TableSpec.
    TableJoinVisitor tableJoinVisitor = new TableJoinVisitor();
    for (InputOperatorSpec inputOpSpec : inputOpSpecs) {
      traverse(inputOpSpec, tableJoinVisitor, opSpec -> opSpec.getRegisteredOperatorSpecs());
    }

    Multimap<SendToTableOperatorSpec, StreamTableJoinOperatorSpec> sendToTableOpSpecToStreamTableJoinOpSpecs =
        tableJoinVisitor.getSendToTableOpSpecToStreamTableJoinOpSpecs();

    return operatorSpec -> {
      // If this is a SendToTableOperatorSpec, return all StreamTableJoinSpecs referencing the same TableSpec.
      // For all other types of operator specs, return the next registered operator specs.
      if (operatorSpec instanceof SendToTableOperatorSpec) {
        SendToTableOperatorSpec sendToTableOperatorSpec = (SendToTableOperatorSpec) operatorSpec;
        return Collections.unmodifiableCollection(sendToTableOpSpecToStreamTableJoinOpSpecs.get(sendToTableOperatorSpec));
      }

      return operatorSpec.getRegisteredOperatorSpecs();
    };
  }

  /**
   * An {@link OperatorSpec} visitor that records all {@link JoinOperatorSpec}s and {@link StreamTableJoinOperatorSpec}s
   * encountered in the graph.
   */
  private static class JoinVisitor implements Consumer<OperatorSpec> {
    private Set<OperatorSpec> joinOpSpecs = new HashSet<>();

    @Override
    public void accept(OperatorSpec opSpec) {
      if (opSpec instanceof JoinOperatorSpec || opSpec instanceof StreamTableJoinOperatorSpec) {
        joinOpSpecs.add(opSpec);
      }
    }

    public Set<OperatorSpec> getJoins() {
      return Collections.unmodifiableSet(joinOpSpecs);
    }
  }

  /**
   * An {@link OperatorSpec} visitor that records associations between every {@link SendToTableOperatorSpec}
   * and all {@link StreamTableJoinOperatorSpec}s that reference the same {@link TableSpec}.
   */
  private static class TableJoinVisitor implements Consumer<OperatorSpec> {
    private final Multimap<TableSpec, SendToTableOperatorSpec> tableSpecToSendToTableOpSpecs = HashMultimap.create();
    private final Multimap<TableSpec, StreamTableJoinOperatorSpec> tableSpecToStreamTableJoinOpSpecs = HashMultimap.create();

    @Override
    public void accept(OperatorSpec opSpec) {
      // Record all SendToTableOperatorSpecs, StreamTableJoinOperatorSpecs, and their corresponding TableSpecs.
      if (opSpec instanceof SendToTableOperatorSpec) {
        SendToTableOperatorSpec sendToTableOperatorSpec = (SendToTableOperatorSpec) opSpec;
        tableSpecToSendToTableOpSpecs.put(sendToTableOperatorSpec.getTableSpec(), sendToTableOperatorSpec);
      } else if (opSpec instanceof StreamTableJoinOperatorSpec) {
        StreamTableJoinOperatorSpec streamTableJoinOpSpec = (StreamTableJoinOperatorSpec) opSpec;
        tableSpecToStreamTableJoinOpSpecs.put(streamTableJoinOpSpec.getTableSpec(), streamTableJoinOpSpec);
      }
    }

    public Multimap<SendToTableOperatorSpec, StreamTableJoinOperatorSpec> getSendToTableOpSpecToStreamTableJoinOpSpecs() {
      Multimap<SendToTableOperatorSpec, StreamTableJoinOperatorSpec> sendToTableOpSpecToStreamTableJoinOpSpecs =
          HashMultimap.create();

      // Map every SendToTableOperatorSpec to all StreamTableJoinOperatorSpecs referencing the same TableSpec.
      for (TableSpec tableSpec : tableSpecToSendToTableOpSpecs.keySet()) {
        Collection<SendToTableOperatorSpec> sendToTableOpSpecs = tableSpecToSendToTableOpSpecs.get(tableSpec);
        Collection<StreamTableJoinOperatorSpec> streamTableJoinOpSpecs =
            tableSpecToStreamTableJoinOpSpecs.get(tableSpec);

        for (SendToTableOperatorSpec sendToTableOpSpec : sendToTableOpSpecs) {
          sendToTableOpSpecToStreamTableJoinOpSpecs.putAll(sendToTableOpSpec, streamTableJoinOpSpecs);
        }
      }

      return Multimaps.unmodifiableMultimap(sendToTableOpSpecToStreamTableJoinOpSpecs);
    }
  }
}
