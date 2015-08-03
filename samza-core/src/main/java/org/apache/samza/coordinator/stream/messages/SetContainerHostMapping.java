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

package org.apache.samza.coordinator.stream.messages;

/**
 * SetContainerHostMapping is used internally by the Samza framework to
 * persist the container-to-host mappings.
 *
 * Structure of the message looks like:
 * {
 *     Key: $ContainerId
 *     Type: set-container-host-assignment
 *     Source: "SamzaContainer-$ContainerId"
 *     MessageMap:
 *     {
 *         ip: InetAddressString,
 *         jmx-url: jmxAddressString
 *         jmx-tunneling-url: jmxTunnelingAddressString
 *     }
 * }
 * */
public class SetContainerHostMapping extends CoordinatorStreamMessage {
  public static final String TYPE = "set-container-host-assignment";
  public static final String IP_KEY = "ip";
  public static final String JMX_URL_KEY = "jmx-url";
  public static final String JMX_TUNNELING_URL_KEY = "jmx-tunneling-url";

  /**
   * SteContainerToHostMapping is used to set the container to host mapping information.
   * @param message which holds the container to host information.
   */
  public SetContainerHostMapping(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * SteContainerToHostMapping is used to set the container to host mapping information.
   * @param source the source of the message
   * @param key the key which is used to persist the message
   * @param hostHttpAddress the IP address of the container
   * @param jmxAddress the JMX address of the container
   * @param jmxTunnelingAddress the JMX tunneling address of the container
   */
  public SetContainerHostMapping(String source, String key, String hostHttpAddress, String jmxAddress, String jmxTunnelingAddress) {
    super(source);
    setType(TYPE);
    setKey(key);
    putMessageValue(IP_KEY, hostHttpAddress);
    putMessageValue(JMX_URL_KEY, jmxAddress);
    putMessageValue(JMX_TUNNELING_URL_KEY, jmxTunnelingAddress);
  }

  /**
   * Returns the IP address of the container.
   * @return the container IP address
   */
  public String getHostLocality() {
    return getMessageValue(IP_KEY);

  }

  /**
   * Returns the JMX url of the container.
   * @return the JMX url
   */
  public String getJmxUrl() {
    return getMessageValue(JMX_URL_KEY);
  }

  /**
   * Returns the JMX tunneling url of the container
   * @return the JMX tunneling url
   */
  public String getJmxTunnelingUrl() {
    return getMessageValue(JMX_TUNNELING_URL_KEY);
  }

}
