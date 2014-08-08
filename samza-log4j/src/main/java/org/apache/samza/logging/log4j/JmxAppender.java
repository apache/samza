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

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

/**
 * <p>
 * JmxAppender is a simple class that exposes Log4J's getLevel and setLevel APIs
 * through a JMX MBean. To enable this MBean, simply include the appender in
 * log4j.xml:
 * </p>
 *
 * <code>
 * &lt;appender name="jmx" class="org.apache.samza.logging.log4j.JmxAppender"/&gt;
 * </code>
 *
 * <p>
 * And then enable it as a root logger:
 * </p>
 *
 * <code>
 *   &lt;root&gt;
 *     &lt;!-- ...other stuff... --&gt;
 *     &lt;appender-ref ref="jmx" /&gt;
 *   &lt;/root&gt;
 * </code>
 */
public class JmxAppender extends AppenderSkeleton {
  public static final String JMX_OBJECT_DOMAIN = JmxAppender.class.getName();
  public static final String JMX_OBJECT_TYPE = "jmx-log4j-appender";
  public static final String JMX_OBJECT_NAME = "jmx-log4j-appender";

  private static final Logger log = Logger.getLogger(JmxAppender.class.getName());

  public JmxAppender() {
    this(ManagementFactory.getPlatformMBeanServer());
  }

  /**
   * Calling the default constructor causes this appender to register JmxLog4J
   * as a JMX MBean.
   * 
   * @param mbeanServer to be injected for unit testing.
   */
  public JmxAppender(MBeanServer mbeanServer) {
    super();

    try {
      JmxLog4J mbean = new JmxLog4J();
      ObjectName name = new ObjectName(JMX_OBJECT_DOMAIN + ":type=" + JMX_OBJECT_TYPE + ",name=" + JMX_OBJECT_NAME);
      mbeanServer.registerMBean(mbean, name);
    } catch (Exception e) {
      log.error("Unable to register Log4J MBean.", e);
    }
  }

  public void close() {
    log.debug("Ignoring close call.");
  }

  public boolean requiresLayout() {
    log.debug("Ignoring requresLayout call.");

    return false;
  }

  protected void append(LoggingEvent event) {
    // No op. We're just using the appender as a convenient way to start the
    // JmxServer without introducing any dependencies anywhere for Log4J.
  }

  /**
   * An MBean to expose Log4J's getLevel and setLevel APIs.
   */
  public static interface JmxLog4JMBean {
    public void setLevel(String level);

    public String getLevel();
  }

  /**
   * An implementation of JmxLog4JMBean that calls getLevel and setLevel on the
   * root logger.
   */
  public static class JmxLog4J implements JmxLog4JMBean {
    public void setLevel(String level) {
      try {
        LogManager.getRootLogger().setLevel(Level.toLevel(level));
      } catch (Exception e) {
        log.error("Unable to set level to: " + level, e);
      }
    }

    public String getLevel() {
      return LogManager.getRootLogger().getLevel().toString();
    }
  }
}
