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
package org.apache.samza.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import com.google.common.collect.Lists;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Util {
  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  static final String FALLBACK_VERSION = "0.0.1";

  /**
   * Make an environment variable string safe to pass.
   */
  public static String envVarEscape(String str) {
    return str
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("`", "\\`");
  }

  public static String getSamzaVersion() {
    return Optional.ofNullable(Util.class.getPackage().getImplementationVersion()).orElseGet(() -> {
      LOG.warn("Unable to find implementation samza version in jar's meta info. Defaulting to {}", FALLBACK_VERSION);
      return FALLBACK_VERSION;
    });
  }

  public static String getTaskClassVersion(Config config) {
    try {
      Optional<String> appClass = Optional.ofNullable(new ApplicationConfig(config).getAppClass());
      if (appClass.isPresent()) {
        return Optional.ofNullable(Class.forName(appClass.get()).getPackage().getImplementationVersion())
            .orElse(FALLBACK_VERSION);
      } else {
        Optional<String> taskClass = new TaskConfig(config).getTaskClass();
        if (taskClass.isPresent()) {
          return Optional.ofNullable(Class.forName(taskClass.get()).getPackage().getImplementationVersion())
              .orElse(FALLBACK_VERSION);
        } else {
          LOG.warn("Unable to find app class or task class. Defaulting to {}", FALLBACK_VERSION);
          return FALLBACK_VERSION;
        }
      }
    } catch (Exception e) {
      LOG.warn(String.format("Ran into exception while trying to get version of app or task. Defaulting to %s",
          FALLBACK_VERSION), e);
      return FALLBACK_VERSION;
    }
  }

  /**
   * Returns the the first host address which is not the loopback address, or {@link InetAddress#getLocalHost} as a
   * fallback.
   *
   * @return the {@link InetAddress} which represents the localhost
   */
  public static InetAddress getLocalHost() {
    try {
      return doGetLocalHost();
    } catch (Exception e) {
      throw new SamzaException("Error while getting localhost", e);
    }
  }

  private static InetAddress doGetLocalHost() throws UnknownHostException, SocketException {
    InetAddress localHost = InetAddress.getLocalHost();
    if (localHost.isLoopbackAddress()) {
      LOG.debug("Hostname {} resolves to a loopback address, trying to resolve an external IP address.",
          localHost.getHostName());
      List<NetworkInterface> networkInterfaces;
      if (System.getProperty("os.name").startsWith("Windows")) {
        networkInterfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
      } else {
        networkInterfaces = Lists.reverse(Collections.list(NetworkInterface.getNetworkInterfaces()));
      }
      for (NetworkInterface networkInterface : networkInterfaces) {
        List<InetAddress> addresses = Collections.list(networkInterface.getInetAddresses())
            .stream()
            .filter(address -> !(address.isLinkLocalAddress() || address.isLoopbackAddress()))
            .collect(Collectors.toList());
        if (!addresses.isEmpty()) {
          InetAddress address = addresses.stream()
              .filter(addr -> addr instanceof Inet4Address)
              .findFirst()
              .orElseGet(() -> addresses.get(0));
          LOG.debug("Found an external IP address {} which represents the localhost.", address.getHostAddress());
          return InetAddress.getByAddress(address.getAddress());
        }
      }
    }
    return localHost;
  }
}