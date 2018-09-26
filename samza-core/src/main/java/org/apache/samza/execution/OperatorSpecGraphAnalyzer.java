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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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
   * Returns a grouping of {@link InputOperatorSpec}s by the joins, i.e. {@link JoinOperatorSpec}s, they participate in.
   */
  public static Multimap<JoinOperatorSpec, InputOperatorSpec> getJoinToInputOperatorSpecs(
      OperatorSpecGraph operatorSpecGraph) {

    Multimap<JoinOperatorSpec, InputOperatorSpec> joinOpSpecToInputOpSpecs = HashMultimap.create();

    // Traverse graph starting from every input operator spec, observing connectivity between input operator specs
    // and Join operator specs.
    Iterable<InputOperatorSpec> inputOpSpecs = operatorSpecGraph.getInputOperators().values();
    for (InputOperatorSpec inputOpSpec : inputOpSpecs) {
      // Observe all join operator specs reachable from this input operator spec.
      JoinOperatorSpecVisitor joinOperatorSpecVisitor = new JoinOperatorSpecVisitor();
      traverse(inputOpSpec, joinOperatorSpecVisitor, opSpec -> opSpec.getRegisteredOperatorSpecs());

      // Associate every encountered join operator spec with this input operator spec.
      for (JoinOperatorSpec joinOpSpec : joinOperatorSpecVisitor.getJoinOperatorSpecs()) {
        joinOpSpecToInputOpSpecs.put(joinOpSpec, inputOpSpec);
      }
    }

    return joinOpSpecToInputOpSpecs;
  }

  /**
   * Traverses {@link OperatorSpec}s starting from {@code startOpSpec}, invoking {@code visitor} with every encountered
   * {@link OperatorSpec}, and using {@code getNextOpSpecs} to determine the set of {@link OperatorSpec}s to visit next.
   */
  private static void traverse(OperatorSpec startOpSpec, Consumer<OperatorSpec> visitor,
      Function<OperatorSpec, Collection<OperatorSpec>> getNextOpSpecs) {
    visitor.accept(startOpSpec);
    for (OperatorSpec nextOpSpec : getNextOpSpecs.apply(startOpSpec)) {
      traverse(nextOpSpec, visitor, getNextOpSpecs);
    }
  }

  /**
   * An {@link OperatorSpecGraph} visitor that records all {@link JoinOperatorSpec}s encountered in the graph.
   */
  private static class JoinOperatorSpecVisitor implements Consumer<OperatorSpec> {
    private Set<JoinOperatorSpec> joinOpSpecs = new HashSet<>();

    @Override
    public void accept(OperatorSpec operatorSpec) {
      if (operatorSpec instanceof JoinOperatorSpec) {
        joinOpSpecs.add((JoinOperatorSpec) operatorSpec);
      }
    }

    public Set<JoinOperatorSpec> getJoinOperatorSpecs() {
      return Collections.unmodifiableSet(joinOpSpecs);
    }
  }
}
