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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest(Util.class) // need this to be able to use powermock with system classes like InetAddress
public class TestUtil {
  @Test
  public void testEnvVarEscape() {
    // no special characters in original
    String noSpecialCharacters = "hello world 123 .?! '";
    assertEquals(noSpecialCharacters, Util.envVarEscape(noSpecialCharacters));

    String withSpecialCharacters = "quotation \" backslash \\ grave accent `";
    String escaped = "quotation \\\" backslash \\\\ grave accent \\`";
    assertEquals(escaped, Util.envVarEscape(withSpecialCharacters));
  }

  /**
   *  It's difficult to explicitly test having an actual version and using the fallback, due to the usage of methods of
   *  Class.
   */
  @Test
  public void testGetSamzaVersion() {
    String utilImplementationVersion = Util.class.getPackage().getImplementationVersion();
    String expectedVersion =
        (utilImplementationVersion != null) ? utilImplementationVersion : Util.FALLBACK_VERSION;
    assertEquals(expectedVersion, Util.getSamzaVersion());
  }

  /**
   *  It's difficult to explicitly test having an actual version and using the fallback, due to the usage of methods of
   *  Class.
   */
  @Test
  public void testGetTaskClassVersion() {
    // cannot find app nor task
    assertEquals(Util.FALLBACK_VERSION, Util.getTaskClassVersion(new MapConfig()));

    // only app
    String appClassVersion = MyAppClass.class.getPackage().getImplementationVersion();
    String expectedAppClassVersion = (appClassVersion != null) ? appClassVersion : Util.FALLBACK_VERSION;
    Config config = new MapConfig(ImmutableMap.of(ApplicationConfig.APP_CLASS, MyAppClass.class.getName()));
    assertEquals(expectedAppClassVersion, Util.getTaskClassVersion(config));

    // only task
    String taskClassVersion = MyTaskClass.class.getPackage().getImplementationVersion();
    String expectedTaskClassVersion = (taskClassVersion != null) ? taskClassVersion : Util.FALLBACK_VERSION;
    config = new MapConfig(ImmutableMap.of(TaskConfig.TASK_CLASS, MyTaskClass.class.getName()));
    assertEquals(expectedTaskClassVersion, Util.getTaskClassVersion(config));

    // both app and task; choose app
    config = new MapConfig(ImmutableMap.of(ApplicationConfig.APP_CLASS, MyAppClass.class.getName(),
        // shouldn't even try to load the task class
        TaskConfig.TASK_CLASS, "this_is_not_a_class"));
    assertEquals(expectedAppClassVersion, Util.getTaskClassVersion(config));
  }

  @Test
  public void testGetLocalHostNotLoopbackAddress() throws UnknownHostException {
    mockStatic(InetAddress.class);
    InetAddress inetAddressLocalHost = mock(InetAddress.class);
    when(inetAddressLocalHost.isLoopbackAddress()).thenReturn(false);
    when(InetAddress.getLocalHost()).thenReturn(inetAddressLocalHost);
    assertEquals(inetAddressLocalHost, Util.getLocalHost());
  }

  @Test
  public void testGetLocalHostLoopbackAddressNoExternalAddressFound() throws Exception {
    mockStatic(InetAddress.class, NetworkInterface.class);
    InetAddress inetAddressLocalHost = mock(InetAddress.class);
    when(inetAddressLocalHost.isLoopbackAddress()).thenReturn(true);
    when(InetAddress.getLocalHost()).thenReturn(inetAddressLocalHost);

    // network interfaces return addresses which are not external
    InetAddress linkLocalAddress = mock(InetAddress.class);
    when(linkLocalAddress.isLinkLocalAddress()).thenReturn(true);
    InetAddress loopbackAddress = mock(InetAddress.class);
    when(loopbackAddress.isLinkLocalAddress()).thenReturn(false);
    when(loopbackAddress.isLoopbackAddress()).thenReturn(true);
    NetworkInterface networkInterface0 = mock(NetworkInterface.class);
    when(networkInterface0.getInetAddresses()).thenReturn(
        Collections.enumeration(Arrays.asList(linkLocalAddress, loopbackAddress)));
    NetworkInterface networkInterface1 = mock(NetworkInterface.class);
    when(networkInterface1.getInetAddresses()).thenReturn(
        Collections.enumeration(Collections.singletonList(loopbackAddress)));
    when(NetworkInterface.getNetworkInterfaces()).thenReturn(
        Collections.enumeration(Arrays.asList(networkInterface0, networkInterface1)));

    assertEquals(inetAddressLocalHost, Util.getLocalHost());
  }

  @Test
  public void testGetLocalHostExternalInet4Address() throws Exception {
    mockStatic(InetAddress.class, NetworkInterface.class);
    InetAddress inetAddressLocalHost = mock(InetAddress.class);
    when(inetAddressLocalHost.isLoopbackAddress()).thenReturn(true);
    when(InetAddress.getLocalHost()).thenReturn(inetAddressLocalHost);

    InetAddress linkLocalAddress = mock(InetAddress.class);
    when(linkLocalAddress.isLinkLocalAddress()).thenReturn(true);
    Inet4Address externalInet4Address = mock(Inet4Address.class);
    when(externalInet4Address.isLinkLocalAddress()).thenReturn(false);
    when(externalInet4Address.isLoopbackAddress()).thenReturn(false);
    byte[] externalInet4AddressBytes = new byte[]{0, 1, 2, 3};
    when(externalInet4Address.getAddress()).thenReturn(externalInet4AddressBytes);
    InetAddress otherExternalAddress = mock(InetAddress.class); // not Inet4Address
    when(otherExternalAddress.isLinkLocalAddress()).thenReturn(false);
    when(otherExternalAddress.isLoopbackAddress()).thenReturn(false);

    NetworkInterface networkInterfaceLinkLocal = mock(NetworkInterface.class);
    when(networkInterfaceLinkLocal.getInetAddresses()).thenReturn(
        Collections.enumeration(Collections.singletonList(linkLocalAddress)));
    NetworkInterface networkInterfaceExternal = mock(NetworkInterface.class);
    when(networkInterfaceExternal.getInetAddresses()).thenReturn(
        Collections.enumeration(Arrays.asList(otherExternalAddress, externalInet4Address)));
    when(NetworkInterface.getNetworkInterfaces()).thenReturn(
        Collections.enumeration(Arrays.asList(networkInterfaceLinkLocal, networkInterfaceExternal)));

    InetAddress finalInetAddress = mock(InetAddress.class);
    when(InetAddress.getByAddress(aryEq(externalInet4AddressBytes))).thenReturn(finalInetAddress);

    assertEquals(finalInetAddress, Util.getLocalHost());
  }

  @Test
  public void testGetLocalHostExternalAddressNotInet4Address() throws Exception {
    mockStatic(InetAddress.class, NetworkInterface.class);
    InetAddress inetAddressLocalHost = mock(InetAddress.class);
    when(inetAddressLocalHost.isLoopbackAddress()).thenReturn(true);
    when(InetAddress.getLocalHost()).thenReturn(inetAddressLocalHost);

    byte[] externalAddressBytes = new byte[]{0, 1, 2, 3, 4, 5};
    InetAddress externalAddress = mock(InetAddress.class);
    when(externalAddress.isLinkLocalAddress()).thenReturn(false);
    when(externalAddress.isLoopbackAddress()).thenReturn(false);
    when(externalAddress.getAddress()).thenReturn(externalAddressBytes);

    NetworkInterface networkInterface = mock(NetworkInterface.class);
    when(networkInterface.getInetAddresses()).thenReturn(
        Collections.enumeration(Collections.singletonList(externalAddress)));
    when(NetworkInterface.getNetworkInterfaces()).thenReturn(
        Collections.enumeration(Collections.singletonList(networkInterface)));

    InetAddress finalInetAddress = mock(InetAddress.class);
    when(InetAddress.getByAddress(aryEq(externalAddressBytes))).thenReturn(finalInetAddress);

    assertEquals(finalInetAddress, Util.getLocalHost());
  }

  /**
   * No requirement for this test that this extends any other class. Just need some placeholder class.
   */
  public static class MyAppClass {
  }

  /**
   * No requirement for this test that this extends any other class. Just need some placeholder class.
   */
  public static class MyTaskClass {
  }
}
