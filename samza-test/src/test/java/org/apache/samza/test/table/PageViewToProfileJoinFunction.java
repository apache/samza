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
package org.apache.samza.test.table;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.task.TaskContext;
import org.apache.samza.test.table.TestTableData.EnrichedPageView;
import org.apache.samza.test.table.TestTableData.PageView;
import org.apache.samza.test.table.TestTableData.Profile;

/**
 * A {@link StreamTableJoinFunction} used by unit tests in this package
 */
class PageViewToProfileJoinFunction implements StreamTableJoinFunction
    <Integer, KV<Integer, PageView>, KV<Integer, Profile>, EnrichedPageView> {
  static Map<String, AtomicInteger> counterPerJoinFn = new HashMap<>();
  private final String joinOpName;

  public PageViewToProfileJoinFunction(String joinOpName) {
    this.joinOpName = joinOpName;
  }

  @Override
  public void init(Config config, TaskContext context) {
    counterPerJoinFn.put(this.joinOpName, new AtomicInteger(0));
  }

  @Override
  public TestTableData.EnrichedPageView apply(KV<Integer, TestTableData.PageView> m, KV<Integer, TestTableData.Profile> r) {
    counterPerJoinFn.get(this.joinOpName).incrementAndGet();
    return r == null ? null : new TestTableData.EnrichedPageView(m.getValue().getPageKey(), m.getKey(), r.getValue().getCompany());
  }

  @Override
  public Integer getMessageKey(KV<Integer, TestTableData.PageView> message) {
    return message.getKey();
  }

  @Override
  public Integer getRecordKey(KV<Integer, TestTableData.Profile> record) {
    return record.getKey();
  }

  public static void reset() {
    counterPerJoinFn.clear();
  }
}
