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
package org.apache.samza.table;

import org.apache.samza.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for all table provider implementations.
 */
abstract public class BaseTableProvider implements TableProvider {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected final String tableId;

  protected Context context;

  /**
   * Construct the table provider using table Id and job configuration
   * @param tableId Id of the table
   */
  public BaseTableProvider(String tableId) {
    this.tableId = tableId;
  }

  @Override
  public void init(Context context) {
    this.context = context;
    logger.info("Initializing table provider for table " + tableId);
  }

  @Override
  public void close() {
    logger.info("Closing table provider for table " + tableId);
  }
}
