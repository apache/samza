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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelRoot;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.dsl.SamzaSqlDslConverter;
import org.apache.samza.sql.translator.QueryTranslator;
import org.apache.samza.sql.translator.TranslatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Entry point for the SamzaSQl stream application that takes in SQL as input and performs stream processing.
 */
public class SamzaSqlApplication implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlApplication.class);

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    try {
      // TODO: Introduce an API to return a dsl string containing one or more sql statements.
      List<String> dslStmts = SamzaSqlDslConverter.fetchSqlFromConfig(appDescriptor.getConfig());

      Map<Integer, TranslatorContext> translatorContextMap = new HashMap<>();

      // 1. Get Calcite plan
      List<String> inputSystemStreams = new LinkedList<>();
      List<String> outputSystemStreams = new LinkedList<>();

      Collection<RelRoot> relRoots =
          SamzaSqlApplicationConfig.populateSystemStreamsAndGetRelRoots(dslStmts, appDescriptor.getConfig(),
              inputSystemStreams, outputSystemStreams);

      // 2. Populate configs
      SamzaSqlApplicationConfig sqlConfig =
          new SamzaSqlApplicationConfig(appDescriptor.getConfig(), inputSystemStreams, outputSystemStreams);

      // 3. Translate Calcite plan to Samza stream operators
      QueryTranslator queryTranslator = new QueryTranslator(appDescriptor, sqlConfig);
      SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(sqlConfig);
      // QueryId implies the index of the query in multiple query statements scenario. It should always start with 0.
      int queryId = 0;
      for (RelRoot relRoot : relRoots) {
        LOG.info("Translating relRoot {} to samza stream graph with queryId {}", relRoot, queryId);
        TranslatorContext translatorContext = new TranslatorContext(appDescriptor, relRoot, executionContext);
        translatorContextMap.put(queryId, translatorContext);
        queryTranslator.translate(relRoot, translatorContext, queryId);
        queryId++;
      }

      // 4. Set all translator contexts
      /*
       * TODO When serialization of ApplicationDescriptor is actually needed, then something will need to be updated here,
       * since translatorContext is not Serializable. Currently, a new ApplicationDescriptor instance is created in each
       * container, so it does not need to be serialized. Therefore, the translatorContext is recreated in each container
       * and does not need to be serialized.
       */
      appDescriptor.withApplicationTaskContextFactory((jobContext,
          containerContext,
          taskContext,
          applicationContainerContext) ->
          new SamzaSqlApplicationContext(translatorContextMap));
    } catch (RuntimeException e) {
      LOG.error("SamzaSqlApplication threw exception.", e);
      throw e;
    }
  }
}
