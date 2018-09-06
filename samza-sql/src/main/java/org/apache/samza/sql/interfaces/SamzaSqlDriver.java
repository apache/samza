/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.samza.sql.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.Driver;


/**
 * Calcite JDBC driver for SamzaSQL which takes in a {@link JavaTypeFactory}
 */
public class SamzaSqlDriver extends Driver {

  private JavaTypeFactory typeFactory;

  public SamzaSqlDriver(JavaTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    final String prefix = getConnectStringPrefix();
    assert url.startsWith(prefix);
    final String urlSuffix = url.substring(prefix.length());
    final Properties info2 = ConnectStringParser.parse(urlSuffix, info);
    final AvaticaConnection connection =
        ((CalciteFactory) factory).newConnection(this, factory, url, info2, null, typeFactory);
    handler.onConnectionInit(connection);
    return connection;
  }
}
