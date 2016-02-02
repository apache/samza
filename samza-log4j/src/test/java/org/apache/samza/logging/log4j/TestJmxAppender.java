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

package org.apache.samza.logging.log4j;

import static org.junit.Assert.assertEquals;

import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * These tests assume that log4j.xml and log4j are both set on the classpath
 * with the JmxAppender added as a root-level appender.
 */
public class TestJmxAppender {
  public static final int PORT = 5500;
  public static final JMXServiceURL URL = getJmxServiceURL();
  private static JMXConnectorServer cs = null;
  private static final Logger log = Logger.getLogger(TestJmxAppender.class.getName());

  private static JMXServiceURL getJmxServiceURL() {
    try {
      return new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:" + PORT + "/jmxapitestrmi");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeSetupServers() throws Exception {
    LocateRegistry.createRegistry(PORT);
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    cs = JMXConnectorServerFactory.newJMXConnectorServer(URL, null, mbeanServer);
    cs.start();
  }

  @AfterClass
  public static void afterCleanLogDirs() throws Exception {
    if (cs != null) {
      cs.stop();
    }
  }

  @Test
  public void testJmxAppender() throws Exception {
    MBeanServerConnection mbserver = JMXConnectorFactory.connect(URL).getMBeanServerConnection();
    ObjectName objectName = new ObjectName(JmxAppender.JMX_OBJECT_DOMAIN + ":type=" + JmxAppender.JMX_OBJECT_TYPE + ",name=" + JmxAppender.JMX_OBJECT_NAME);
    String level = null;
    MockAppender mockAppender = new MockAppender();
    Logger.getRootLogger().addAppender(mockAppender);

    // Check INFO is set (from log4j.xml).
    level = (String) mbserver.getAttribute(objectName, "Level");
    assertEquals("INFO", level);

    log.info("info1");
    log.debug("debug1");

    // Set to debug.
    mbserver.setAttribute(objectName, new Attribute("Level", "debug"));

    // Check DEBUG is set.
    level = (String) mbserver.getAttribute(objectName, "Level");
    assertEquals("DEBUG", level);

    log.info("info2");
    log.debug("debug2");

    List<LoggingEvent> logLines = mockAppender.getLogLines();

    // Should not have debug1 because log level is info at first.
    Iterator<LoggingEvent> logLineIterator = logLines.iterator();
    assertEquals(3, logLines.size());
    assertEquals("info1", logLineIterator.next().getMessage());
    assertEquals("info2", logLineIterator.next().getMessage());
    assertEquals("debug2", logLineIterator.next().getMessage());
  }

  public static final class MockAppender extends AppenderSkeleton {
    private final List<LoggingEvent> logLines;

    public MockAppender() {
      logLines = new ArrayList<LoggingEvent>();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(LoggingEvent event) {
      logLines.add(event);
    }

    public List<LoggingEvent> getLogLines() {
      return logLines;
    }
  }
}
