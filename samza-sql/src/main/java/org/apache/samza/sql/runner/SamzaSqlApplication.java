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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelRoot;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.dsl.SamzaSqlDslConverter;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.translator.QueryTranslator;
import org.apache.samza.sql.translator.TranslatorContext;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Entry point for the SamzaSQl stream application that takes in SQL as input and performs stream processing.
 */
public class SamzaSqlApplication implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlApplication.class);
  private AtomicInteger queryId = new AtomicInteger(0);

  @Override
  public void describe(StreamApplicationDescriptor appDesc) {
    try {
      // TODO: Introduce an API to return a dsl string containing one or more sql statements.
      List<String> dslStmts = SamzaSqlDslConverter.fetchSqlFromConfig(appDesc.getConfig());

      Map<Integer, TranslatorContext> translatorContextMap = new HashMap<>();

      // 1. Get Calcite plan
      Set<String> inputSystemStreams = new HashSet<>();
      Set<String> outputSystemStreams = new HashSet<>();

      Collection<RelRoot> relRoots =
          SamzaSqlApplicationConfig.populateSystemStreamsAndGetRelRoots(dslStmts, appDesc.getConfig(),
              inputSystemStreams, outputSystemStreams);

      // 2. Populate configs
      SamzaSqlApplicationConfig sqlConfig =
          new SamzaSqlApplicationConfig(appDesc.getConfig(), inputSystemStreams, outputSystemStreams);

      // 3. Translate Calcite plan to Samza stream operators
      QueryTranslator queryTranslator = new QueryTranslator(appDesc, sqlConfig);
      SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(sqlConfig);
      Map<String, SamzaRelConverter> converters = sqlConfig.getSamzaRelConverters();
      for (RelRoot relRoot : relRoots) {
        LOG.info("Translating relRoot {} to samza stream graph", relRoot);
        int qId = queryId.incrementAndGet();
        TranslatorContext translatorContext = new TranslatorContext(appDesc, relRoot, executionContext, converters);
        translatorContextMap.put(qId, translatorContext);
        queryTranslator.translate(relRoot, translatorContext, qId);
      }

      // 4. Set all translator contexts in TaskContext
      appDesc.withContextManager(new ContextManager() {
        @Override
        public void init(Config config, TaskContext taskContext) {
          taskContext.setUserContext(translatorContextMap);
        }

        @Override
        public void close() {
        }
      });
    } catch (RuntimeException e) {
      LOG.error("SamzaSqlApplication threw exception.", e);
      throw e;
    }
  }
}
