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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;


/**
 * A utility class that encapsulates the logic for traversing an {@link OperatorSpecGraph} and building
 * associations between related {@link OperatorSpec}s.
 */
/* package private */ class OperatorSpecGraphAnalyzer {

  /**
   * Returns a grouping of {@link InputOperatorSpec}s participating in Join operations in the provided
   * {@code operatorSpecGraph} by the {@link JoinOperatorSpec}s corresponding to these operations.
   */
  public static Multimap<JoinOperatorSpec, InputOperatorSpec> getJoinedInputOperatorSpecs(OperatorSpecGraph operatorSpecGraph) {
    JoinedInputOperatorSpecVisitor joinedInputOperatorSpecVisitor = new JoinedInputOperatorSpecVisitor();

    // Traverse graph starting from every input operator spec, observing
    // connectivity between input operator specs and Join operator specs.
    Iterable<InputOperatorSpec> inputOperatorSpecs = operatorSpecGraph.getInputOperators().values();
    for (InputOperatorSpec inputOperatorSpec : inputOperatorSpecs) {
      traverse(inputOperatorSpec, joinedInputOperatorSpecVisitor, OperatorSpec::getRegisteredOperatorSpecs);
    }

    return joinedInputOperatorSpecVisitor.getJoinOpSpecToInputOpSpecs();
  }

  /**
   * Traverses graph starting from {@code vertex}, invoking {@code visitor} with every encountered vertex,
   * and using {@code getNextVertexes} to determine next set of vertexes to visit.
   */
  private static <T> void traverse(T vertex, Consumer<T> visitor, Function<T, Iterable<? extends T>> getNextVertexes) {
    visitor.accept(vertex);
    for (T nextVertex : getNextVertexes.apply(vertex)) {
      traverse(nextVertex, visitor, getNextVertexes);
    }
  }

  /**
   * An {@link OperatorSpecGraph} visitor that records associations between {@link InputOperatorSpec}s
   * and the {@link JoinOperatorSpec}s they participate in or lie on a path to.
   */
  private static class JoinedInputOperatorSpecVisitor implements Consumer<OperatorSpec> {
    private final Multimap<JoinOperatorSpec, InputOperatorSpec> joinOpSpecToInputOpSpecs = HashMultimap.create();

    private InputOperatorSpec currentInputOpSpec;

    @Override
    public void accept(OperatorSpec operatorSpec) {
      if (operatorSpec instanceof InputOperatorSpec) {
        currentInputOpSpec = (InputOperatorSpec) operatorSpec;
      } else if (operatorSpec instanceof JoinOperatorSpec) {
        joinOpSpecToInputOpSpecs.put((JoinOperatorSpec) operatorSpec, currentInputOpSpec);
      }
    }

    public Multimap<JoinOperatorSpec, InputOperatorSpec> getJoinOpSpecToInputOpSpecs() {
      return Multimaps.unmodifiableMultimap(joinOpSpecToInputOpSpecs);
    }
  }
}
