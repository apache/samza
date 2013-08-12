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

package samza.examples.wikipedia.system;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

public class WikipediaSystemFactory implements SystemFactory {
  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new WikipediaSystemAdmin();
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    String host = config.get("systems." + systemName + ".host");
    int port = config.getInt("systems." + systemName + ".port");
    WikipediaFeed feed = new WikipediaFeed(host, port);

    return new WikipediaConsumer(systemName, feed, registry);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    throw new SamzaException("You can't produce to a Wikipedia feed! How about making some edits to a Wiki, instead?");
  }
}
