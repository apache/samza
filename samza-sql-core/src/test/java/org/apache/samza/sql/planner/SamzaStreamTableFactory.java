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
package org.apache.samza.sql.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

public class SamzaStreamTableFactory implements TableFactory<Table> {
    public Table create(SchemaPlus schema, String name,
                        Map<String, Object> operand, RelDataType rowType) {
        final RelProtoDataType protoRowType = new RelProtoDataType() {
            public RelDataType apply(RelDataTypeFactory a0) {
                return a0.builder()
                        .add("id", SqlTypeName.INTEGER)
                        .add("product", SqlTypeName.VARCHAR, 10)
                        .add("quantity", SqlTypeName.INTEGER)
                        .build();
            }
        };
        final ImmutableList<Object[]> rows = ImmutableList.of(
                new Object[]{1, "paint", 10},
                new Object[]{2, "paper", 5});

        return new StreamableTable() {
            public Table stream() {
                return new OrdersTable(protoRowType, rows);
            }

            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return protoRowType.apply(typeFactory);
            }

            public Statistic getStatistic() {
                return Statistics.UNKNOWN;
            }

            public Schema.TableType getJdbcTableType() {
                return Schema.TableType.TABLE;
            }
        };
    }

    public static class OrdersTable implements ScannableTable {
        private final RelProtoDataType protoRowType;
        private final ImmutableList<Object[]> rows;

        public OrdersTable(RelProtoDataType protoRowType,
                           ImmutableList<Object[]> rows) {
            this.protoRowType = protoRowType;
            this.rows = rows;
        }

        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(rows);
        }

        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoRowType.apply(typeFactory);
        }

        public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }

        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.STREAM;
        }
    }
}
