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
package org.apache.samza.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.util.Util;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TestQueryPlanner {
  public static final String STREAM_SCHEMA = "     {\n"
      + "       name: 'STREAMS',\n"
      + "       tables: [ {\n"
      + "         type: 'custom',\n"
      + "         name: 'ORDERS',\n"
      + "         stream: {\n"
      + "           stream: true\n"
      + "         },\n"
      + "         factory: '" + SamzaStreamTableFactory.class.getName() + "'\n"
      + "       } ]\n"
      + "     }\n";

  public static final String STREAM_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'STREAMS',\n"
      + "   schemas: [\n"
      + STREAM_SCHEMA
      + "   ]\n"
      + "}";

  public static final String SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_PLAN_EXPECTED =
      "LogicalDelta\n" +
          "  LogicalProject(id=[$0], product=[$1], quantity=[$2])\n" +
          "    LogicalFilter(condition=[>($2, 5)])\n" +
          "      EnumerableTableScan(table=[[STREAMS, ORDERS]])";
  public static final String SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE =
      "select stream * from orders where quantity > 5";

  @Test
  public void testQueryPlanner() throws IOException, SQLException {

    SamzaCalciteConnection connection = new SamzaCalciteConnection(STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE, context);
    Assert.assertNotNull(relNode);
    String s = Util.toLinux(RelOptUtil.toString(relNode));
    Assert.assertTrue(s.contains(SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_PLAN_EXPECTED));
  }

  public static Map<CalciteConnectionProperty, String> defaultConfiguration(){
    Map<CalciteConnectionProperty, String> map = new HashMap<CalciteConnectionProperty, String>();

    map.put(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    map.put(CalciteConnectionProperty.QUOTED_CASING, Casing.UNCHANGED.name());
    map.put(CalciteConnectionProperty.UNQUOTED_CASING, Casing.UNCHANGED.name());
    map.put(CalciteConnectionProperty.QUOTING, Quoting.BACK_TICK.name());

    return map;
  }
}
