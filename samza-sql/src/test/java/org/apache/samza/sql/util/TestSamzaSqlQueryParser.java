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

package org.apache.samza.sql.util;

import org.apache.samza.SamzaException;
import org.apache.samza.sql.util.SamzaSqlQueryParser.QueryInfo;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlQueryParser {

  @Test
  public void testParseQuery() {
    QueryInfo queryInfo = SamzaSqlQueryParser.parseQuery("insert into log.foo select * from tracking.bar");
    Assert.assertEquals("log.foo", queryInfo.getSink());
    Assert.assertEquals(1, queryInfo.getSources().size());
    Assert.assertEquals("tracking.bar", queryInfo.getSources().get(0));
  }

  @Test
  public void testParseGroupyByQuery() {
    QueryInfo queryInfo = SamzaSqlQueryParser.parseQuery("insert into log.foo select b.pageKey, count(*) from tracking.bar as b group by b.pageKey");
    Assert.assertEquals("log.foo", queryInfo.getSink());
    Assert.assertEquals(1, queryInfo.getSources().size());
    Assert.assertEquals("tracking.bar", queryInfo.getSources().get(0));
  }

  @Test
  public void testParseUnNestSubQuery() {
    QueryInfo queryInfo = SamzaSqlQueryParser.parseQuery("insert into log.foo SELECT * FROM unnest(SELECT int_array_field1 FROM tracking.bar) ");
    Assert.assertEquals("log.foo", queryInfo.getSink());
    Assert.assertEquals(1, queryInfo.getSources().size());
    Assert.assertEquals("tracking.bar", queryInfo.getSources().get(0));
  }

  @Test
  public void testParseJoinSubQuery() {
    String sql =
        "Insert into testavro.enrichedPageViewTopic"
            + " select p.name as profileName, pv.pageKey"
            + " from (SELECT * FROM testavro.PAGEVIEW pv1 where pv1.field1='foo') as pv"
            + " join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";
    QueryInfo queryInfo = SamzaSqlQueryParser.parseQuery(sql);
    Assert.assertEquals("testavro.enrichedPageViewTopic", queryInfo.getSink());
    Assert.assertEquals(2, queryInfo.getSources().size());
    Assert.assertEquals("testavro.PAGEVIEW", queryInfo.getSources().get(0));
    Assert.assertEquals("testavro.PROFILE.$table", queryInfo.getSources().get(1));
  }

  @Test
  public void testParseJoinUnNestQuery() {
    String sql =
        "Insert into testavro.enrichedPageViewTopic"
            + " select p.name as profileName, pv.pageKey"
            + " from unnest(SELECT int_array_field1 FROM testavro.PAGEVIEW) as pv"
            + " join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";
    QueryInfo queryInfo = SamzaSqlQueryParser.parseQuery(sql);
    Assert.assertEquals("testavro.enrichedPageViewTopic", queryInfo.getSink());
    Assert.assertEquals(2, queryInfo.getSources().size());
    Assert.assertEquals("testavro.PAGEVIEW", queryInfo.getSources().get(0));
    Assert.assertEquals("testavro.PROFILE.$table", queryInfo.getSources().get(1));
  }

  @Test
  public void testParseJoinQuery() {
    String sql =
        "Insert into testavro.enrichedPageViewTopic"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";
    QueryInfo queryInfo = SamzaSqlQueryParser.parseQuery(sql);
    Assert.assertEquals("testavro.enrichedPageViewTopic", queryInfo.getSink());
    Assert.assertEquals(2, queryInfo.getSources().size());
    Assert.assertEquals("testavro.PAGEVIEW", queryInfo.getSources().get(0));
    Assert.assertEquals("testavro.PROFILE.$table", queryInfo.getSources().get(1));
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
}
