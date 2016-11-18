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

package org.apache.samza.metrics;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to create JMX related objects
 */
public class JmxUtil {
  private static final Logger log = LoggerFactory.getLogger(JmxUtil.class);

  public static ObjectName getObjectName(String group, String name, String t) throws MalformedObjectNameException {
    StringBuilder nameBuilder = new StringBuilder();
    nameBuilder.append(makeNameJmxSafe(group));
    nameBuilder.append(":type=");
    nameBuilder.append(makeNameJmxSafe(t));
    nameBuilder.append(",name=");
    nameBuilder.append(makeNameJmxSafe(name));
    ObjectName objName = new ObjectName(nameBuilder.toString());
    log.debug("Resolved name for " + group + ", " + name + ", " + t + " to: " + objName);
    return objName;
  }

  /*
   * JMX only has ObjectName.quote, which is pretty nasty looking. This
   * function escapes without quoting, using the rules outlined in:
   * http://docs.oracle.com/javase/1.5.0/docs/api/javax/management/ObjectName.html
   */
  public static String makeNameJmxSafe(String str) {
    return str
      .replace(",", "_")
      .replace("=", "_")
      .replace(":", "_")
      .replace("\"", "_")
      .replace("*", "_")
      .replace("?", "_");
  }
}
