package org.apache.samza.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.JobConfig;
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
@PrepareForTest({NetworkingUtil.class})
public class TestUtil {
  private static final String CONFIG_KEY = "config.key";
  private static final String CONFIG_VALUE = "value";
  private static final String NEW_CONFIG_KEY = "new.rewritten.config.key";
  private static final String REWRITER_NAME = "propertyRewriter";
  private static final String OTHER_REWRITER_NAME = "otherPropertyRewriter";

  @Test
  public void testEnvVarEscape() {
    // no special characters in original
    String noSpecialCharacters = "hello world 123 .?!";
    assertEquals(noSpecialCharacters, Util.envVarEscape(noSpecialCharacters));

    String withSpecialCharacters = "quotation \" apostrophe '";
    String escaped = "quotation \\\" apostrophe \\'";
    assertEquals(escaped, Util.envVarEscape(withSpecialCharacters));
  }

  /**
   *  It's difficult to explicitly test having an actual version and using the fallback, due to the usage of methods of
   *  Class.
   */
  @Test
  public void testGetSamzaVersion() {
    String utilImplementationVersion = Util.class.getPackage().getImplementationVersion();
    String expectedVersion = (utilImplementationVersion != null) ? utilImplementationVersion : Util.FALLBACK_VERSION();
    assertEquals(expectedVersion, Util.getSamzaVersion());
  }

  /**
   *  It's difficult to explicitly test having an actual version and using the fallback, due to the usage of methods of
   *  Class.
   */
  @Test
  public void testGetTaskClassVersion() {
    // cannot find app nor task
    assertEquals(Util.FALLBACK_VERSION(), Util.getTaskClassVersion(new MapConfig()));

    // only app
    String appClassVersion = MyAppClass.class.getPackage().getImplementationVersion();
    String expectedAppClassVersion = (appClassVersion != null) ? appClassVersion : Util.FALLBACK_VERSION();
    Config config = new MapConfig(ImmutableMap.of(ApplicationConfig.APP_CLASS, MyAppClass.class.getName()));
    assertEquals(expectedAppClassVersion, Util.getTaskClassVersion(config));

    // only task
    String taskClassVersion = MyTaskClass.class.getPackage().getImplementationVersion();
    String expectedTaskClassVersion = (taskClassVersion != null) ? taskClassVersion : Util.FALLBACK_VERSION();
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

  @Test
  public void testRewriteConfig() {
    Map<String, String> baseConfigMap = ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE);

    // no rewriters
    Map<String, String> fullConfig = new HashMap<>(baseConfigMap);
    assertEquals(fullConfig, Util.rewriteConfig(new MapConfig(fullConfig)));

    // one rewriter
    fullConfig = new HashMap<>(baseConfigMap);
    fullConfig.put(JobConfig.CONFIG_REWRITERS, REWRITER_NAME);
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME), NewPropertyRewriter.class.getName());
    Map<String, String> expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(NEW_CONFIG_KEY, CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), Util.rewriteConfig(new MapConfig(fullConfig)));

    // only apply rewriters from rewriters list
    fullConfig = new HashMap<>(baseConfigMap);
    fullConfig.put(JobConfig.CONFIG_REWRITERS, OTHER_REWRITER_NAME);
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME), NewPropertyRewriter.class.getName());
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, OTHER_REWRITER_NAME),
        UpdatePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(CONFIG_KEY, CONFIG_VALUE + CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), Util.rewriteConfig(new MapConfig(fullConfig)));

    // two rewriters; second rewriter overwrites configs from first
    fullConfig = new HashMap<>(baseConfigMap);
    fullConfig.put(JobConfig.CONFIG_REWRITERS, REWRITER_NAME + "," + OTHER_REWRITER_NAME);
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME), NewPropertyRewriter.class.getName());
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, OTHER_REWRITER_NAME),
        UpdatePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(NEW_CONFIG_KEY, CONFIG_VALUE + CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), Util.rewriteConfig(new MapConfig(fullConfig)));
  }

  /**
   * This fails because Util will interpret the empty string value as a single rewriter which has the empty string as a
   * name, and there is no rewriter class config for a rewriter name which is the empty string.
   * TODO: should this be fixed to interpret the empty string as an empty list?
   */
  @Test(expected = SamzaException.class)
  public void testRewriteConfigConfigRewritersEmptyString() {
    Config config = new MapConfig(ImmutableMap.of(JobConfig.CONFIG_REWRITERS, ""));
    Util.rewriteConfig(config);
  }

  @Test(expected = SamzaException.class)
  public void testRewriteConfigNoClassForConfigRewriterName() {
    Config config =
        new MapConfig(ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, JobConfig.CONFIG_REWRITERS, "unknownRewriter"));
    Util.rewriteConfig(config);
  }

  @Test(expected = SamzaException.class)
  public void testRewriteConfigRewriterClassDoesNotExist() {
    Config config = new MapConfig(ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, JobConfig.CONFIG_REWRITERS, REWRITER_NAME,
        String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME), "not_a_class"));
    Util.rewriteConfig(config);
  }

  @Test
  public void testApplyRewriter() {
    // new property
    Map<String, String> fullConfig =
        ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
            NewPropertyRewriter.class.getName());
    Map<String, String> expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(NEW_CONFIG_KEY, CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), Util.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME));

    // update property
    fullConfig =
        ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
            UpdatePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(CONFIG_KEY, CONFIG_VALUE + CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), Util.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME));

    // remove property
    fullConfig =
        ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
            DeletePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.remove(CONFIG_KEY);
    assertEquals(new MapConfig(expectedConfigMap), Util.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME));
  }

  @Test(expected = SamzaException.class)
  public void testApplyRewriterNoClassForConfigRewriterName() {
    Map<String, String> fullConfig = ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE);
    Util.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME);
  }

  @Test(expected = SamzaException.class)
  public void testApplyRewriterClassDoesNotExist() {
    Map<String, String> fullConfig =
        ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
            "not_a_class");
    Config expectedConfig = new MapConfig(ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, NEW_CONFIG_KEY, CONFIG_VALUE));
    assertEquals(expectedConfig, Util.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME));
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

  /**
   * Adds a new config entry for the key {@link #NEW_CONFIG_KEY} which has the same value as {@link #CONFIG_KEY}.
   */
  public static class NewPropertyRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      ImmutableMap.Builder<String, String> newConfigMapBuilder = new ImmutableMap.Builder<>();
      newConfigMapBuilder.putAll(config);
      newConfigMapBuilder.put(NEW_CONFIG_KEY, config.get(CONFIG_KEY));
      return new MapConfig(newConfigMapBuilder.build());
    }
  }

  /**
   * If an entry at {@link #NEW_CONFIG_KEY} exists, overwrites it to be the value concatenated with itself. Otherwise,
   * updates the entry at {@link #CONFIG_KEY} to be the value concatenated to itself.
   */
  public static class UpdatePropertyRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      Map<String, String> newConfigMap = new HashMap<>(config);
      if (config.containsKey(NEW_CONFIG_KEY)) {
        // for testing overwriting of new configs
        newConfigMap.put(NEW_CONFIG_KEY, config.get(NEW_CONFIG_KEY) + config.get(NEW_CONFIG_KEY));
      } else {
        newConfigMap.put(CONFIG_KEY, config.get(CONFIG_KEY) + config.get(CONFIG_KEY));
      }
      return new MapConfig(newConfigMap);
    }
  }

  /**
   * Removes config entry for the key {@link #CONFIG_KEY} and {@link #NEW_CONFIG_KEY}.
   */
  public static class DeletePropertyRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      Map<String, String> newConfigMap = new HashMap<>(config);
      newConfigMap.remove(CONFIG_KEY);
      return new MapConfig(newConfigMap);
    }
  }
}
