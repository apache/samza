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

package org.apache.samza.sql.testutil;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.samza.sql.testutil.SqlFileParser;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlFileParser {

  public static final String TEST_SQL =
      "insert into log.outputStream \n" + "\tselect * from brooklin.elasticsearchEnterpriseAccounts\n"
          + "insert into log.outputstream select sfdcAccountId as key, organizationUrn as name2, "
          + "description as name3 from brooklin.elasticsearchEnterpriseAccounts\n" + "--insert into log.outputstream \n"
          + "insert into log.outputstream \n" + "\n" + "\tselect id, MyTest(id) as id2 \n" + "\n"
          + "\tfrom tracking.SamzaSqlTestTopic1_p8";

  @Test
  public void testParseSqlFile() throws IOException {
    File tempFile = File.createTempFile("testparser", "");
    PrintWriter fileWriter = new PrintWriter(tempFile.getCanonicalPath());
    fileWriter.println(TEST_SQL);
    fileWriter.close();

    List<String> sqlStmts = SqlFileParser.parseSqlFile(tempFile.getAbsolutePath());
    Assert.assertEquals(3, sqlStmts.size());
    Assert.assertEquals("insert into log.outputStream select * from brooklin.elasticsearchEnterpriseAccounts",
        sqlStmts.get(0));
    Assert.assertEquals(
        "insert into log.outputstream select sfdcAccountId as key, organizationUrn as name2, description as name3 from brooklin.elasticsearchEnterpriseAccounts",
        sqlStmts.get(1));
    Assert.assertEquals("insert into log.outputstream select id, MyTest(id) as id2 from tracking.SamzaSqlTestTopic1_p8",
        sqlStmts.get(2));
  }
}
