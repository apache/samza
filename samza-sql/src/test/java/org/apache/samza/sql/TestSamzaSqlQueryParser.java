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

package org.apache.samza.sql;

import org.apache.samza.SamzaException;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser.QueryInfo;
import org.junit.Test;

import junit.framework.Assert;

public class TestSamzaSqlQueryParser {

  @Test
  public void testParseQuery() {
    QueryInfo queryInfo = SamzaSqlQueryParser.parseQuery("insert into log.foo select * from tracking.bar");
    Assert.assertEquals("log.foo", queryInfo.getOutputSource());
    Assert.assertEquals(queryInfo.getSelectQuery(), "select * from tracking.bar", queryInfo.getSelectQuery());
    Assert.assertEquals(1, queryInfo.getInputSources().size());
    Assert.assertEquals("tracking.bar", queryInfo.getInputSources().get(0));
  }

  @Test
  public void testParseInvalidQuery() {

    try {
      SamzaSqlQueryParser.parseQuery("select * from tracking.bar");
      Assert.fail("Expected a samzaException");
    } catch (SamzaException e) {
    }

    try {
      SamzaSqlQueryParser.parseQuery("insert into select * from tracking.bar");
      Assert.fail("Expected a samzaException");
    } catch (SamzaException e) {
    }

    try {
      SamzaSqlQueryParser.parseQuery("insert into log.off select from tracking.bar");
      Assert.fail("Expected a samzaException");
    } catch (SamzaException e) {
    }
  }

  @Test
  public void testParseJoin() {
    try {
      SamzaSqlQueryParser.parseQuery("insert into log.foo select * from tracking.bar1,tracking.bar2");
      Assert.fail("Expected a samzaException");
    } catch (SamzaException e) {
    }
  }
}
