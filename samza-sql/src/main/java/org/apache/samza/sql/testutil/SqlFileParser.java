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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility to read the .sql file and parse out the various sql statements in the file.
 * Right now Samza SQL
 *     Samza SQL supports a sql file with multiple SQL statements where each SQL statement can be spread across
 *       multiple lines.
 *     It supports sql comments where a line starts with "--".
 *     It cannot support multiple sql statements in a single line.
 *     All the empty lines are ignored
 *     All the SQL statements should start with "insert into".
 *
 * e.g. SQL File
 * -- Sample comment
 * insert into log.output1 select * from kafka.input1
 *
 * insert into log.output2
 *   select * from kafka.input2
 *
 * -- You may have empty lines in between a single query.
 * insert into log.output3
 *
 *   select * from kafka.input3
 *
 * -- Below line which contains multiple sql statements are not supported
 * -- insert into log.output4 select * from kafka.input4 insert into log.output5 select * from kafka.input5
 *
 * -- Below SQL statement is not supported because it doesn't start with insert into
 * -- select * from kafka.input6
 */
public class SqlFileParser {

  private static final String INSERT_CMD = "insert";
  private static final Logger LOG = LoggerFactory.getLogger(SqlFileParser.class);
  private static final String SQL_COMMENT_PREFIX = "--";

  private SqlFileParser() {
  }

  public static List<String> parseSqlFile(String fileName) {
    Validate.notEmpty(fileName, "fileName cannot be empty.");
    List<String> sqlLines;
    try {
      sqlLines = Files.lines(Paths.get(fileName)).collect(Collectors.toList());
    } catch (IOException e) {
      String msg = String.format("Unable to parse the sql file %s", fileName);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
    List<String> sqlStmts = new ArrayList<>();
    String lastStatement = "";
    for (String sqlLine : sqlLines) {
      String sql = sqlLine.trim();
      if (sql.toLowerCase().startsWith(INSERT_CMD)) {
        if (StringUtils.isNotEmpty(lastStatement)) {
          sqlStmts.add(lastStatement);
        }

        lastStatement = sql;
      } else if (StringUtils.isNotBlank(sql) && !sql.startsWith(SQL_COMMENT_PREFIX)) {
        lastStatement = String.format("%s %s", lastStatement, sql);
      }
    }

    if (!StringUtils.isWhitespace(lastStatement)) {
      sqlStmts.add(lastStatement);
    }
    return sqlStmts;
  }
}
