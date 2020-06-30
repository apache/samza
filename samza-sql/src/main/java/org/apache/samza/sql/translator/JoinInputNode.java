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
import java.util.Map;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;


/**
 * This class represents the input node for the join. It can be either a table or a stream.
 */
public class JoinInputNode {

  // Calcite RelNode corresponding to the input
  private final RelNode relNode;
  // Id of the key(s) in the fields that this input has
  private final List<Integer> keyIds;

  private final InputType inputType;
  private final boolean isPosOnRight;

  public enum InputType {
    STREAM,
    LOCAL_TABLE,
    REMOTE_TABLE
  }

  JoinInputNode(RelNode relNode, List<Integer> keyIds, InputType inputType, boolean isPosOnRight) {
    this.relNode = relNode;
    this.keyIds = keyIds;
    this.inputType = inputType;
    this.isPosOnRight = isPosOnRight;
  }

  boolean isRemoteTable() {
    return this.inputType == InputType.REMOTE_TABLE;
  }

  List<Integer> getKeyIds() {
    return keyIds;
  }

  List<String> getFieldNames() {
    return relNode.getRowType().getFieldNames();
  }

  RelNode getRelNode() {
    return relNode;
  }

  String getSourceName() {
    return SqlIOConfig.getSourceFromSourceParts(relNode.getTable().getQualifiedName());
  }

  boolean isPosOnRight() {
    return isPosOnRight;
  }

  public static JoinInputNode.InputType getInputType(
      RelNode relNode, Map<String, SqlIOConfig> systemStreamConfigBySource) {

    // NOTE: Any intermediate form of a join is always a stream. Eg: For the second level join of
    // stream-table-table join, the left side of the join is join output, which we always
    // assume to be a stream. The intermediate stream won't be an instance of TableScan.
    // The join key(s) for the table could be an udf in which case the relNode would be LogicalProject.

    // If the relNode is a vertex in a DAG, get the real relNode. This happens due to query optimization.
    if (relNode instanceof HepRelVertex) {
      relNode = ((HepRelVertex) relNode).getCurrentRel();
    }

    if (relNode instanceof TableScan || relNode instanceof LogicalProject || relNode instanceof LogicalFilter) {
      SqlIOConfig sourceTableConfig = JoinTranslator.resolveSQlIOForTable(relNode, systemStreamConfigBySource);
      if (sourceTableConfig == null || !sourceTableConfig.getTableDescriptor().isPresent()) {
        return JoinInputNode.InputType.STREAM;
      } else if (sourceTableConfig.getTableDescriptor().get() instanceof RemoteTableDescriptor ||
          sourceTableConfig.getTableDescriptor().get() instanceof CachingTableDescriptor) {
        return JoinInputNode.InputType.REMOTE_TABLE;
      } else {
        return JoinInputNode.InputType.LOCAL_TABLE;
      }
    } else {
      return JoinInputNode.InputType.STREAM;
    }
  }

}
