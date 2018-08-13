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

package org.apache.samza.sql.runner;

import java.util.List;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamAppDescriptor;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.sql.translator.QueryTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Entry point for the SamzaSQl stream application that takes in SQL as input and performs stream processing.
 */
public class SamzaSqlApplication implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlApplication.class);

  @Override
  public void describe(StreamAppDescriptor appDesc) {
    try {
      SamzaSqlApplicationConfig sqlConfig = new SamzaSqlApplicationConfig(appDesc.getConfig());
      QueryTranslator queryTranslator = new QueryTranslator(sqlConfig);
      List<SamzaSqlQueryParser.QueryInfo> queries = sqlConfig.getQueryInfo();
      for (SamzaSqlQueryParser.QueryInfo query : queries) {
        LOG.info("Translating the query {} to samza stream graph", query.getSelectQuery());
        queryTranslator.translate(query, appDesc);
      }
    } catch (RuntimeException e) {
      LOG.error("SamzaSqlApplication threw exception.", e);
      throw e;
    }
  }
}
