package org.apache.samza.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import com.google.common.collect.Lists;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.JobConfig;
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
    return str.replace("\"", "\\\"").replace("'", "\\'");
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
      return doGetLocalHost(new NetworkingUtil());
    } catch (Exception e) {
      throw new SamzaException("Error while getting localhost", e);
    }
  }

  private static InetAddress doGetLocalHost(NetworkingUtil networkingUtil)
      throws UnknownHostException, SocketException {
    InetAddress localHost = networkingUtil.inetAddressGetLocalHost();
    if (localHost.isLoopbackAddress()) {
      LOG.debug("Hostname {} resolves to a loopback address, trying to resolve an external IP address.",
          localHost.getHostName());
      List<NetworkInterface> networkInterfaces;
      if (System.getProperty("os.name").startsWith("Windows")) {
        networkInterfaces = Collections.list(networkingUtil.networkInterfaceGetNetworkInterfaces());
      } else {
        networkInterfaces = Lists.reverse(Collections.list(networkingUtil.networkInterfaceGetNetworkInterfaces()));
      }
      for (NetworkInterface networkInterface : networkInterfaces) {
        List<InetAddress> addresses =
            Collections.list(networkingUtil.networkInterfaceGetInetAddresses(networkInterface))
                .stream()
                .filter(address -> !(address.isLinkLocalAddress() || address.isLoopbackAddress()))
                .collect(Collectors.toList());
        if (!addresses.isEmpty()) {
          InetAddress address = addresses.stream()
              .filter(addr -> addr instanceof Inet4Address)
              .findFirst()
              .orElseGet(() -> addresses.get(0));
          LOG.debug("Found an external IP address {} which represents the localhost.",
              networkingUtil.inetAddressGetHostAddress(address));
          return networkingUtil.inetAddressGetByAddress(networkingUtil.inetAddressGetAddress(address));
        }
      }
    }
    return localHost;
  }

  /**
   * Re-writes configuration using a ConfigRewriter, if one is defined. If there is no ConfigRewriter defined for the
   * job, then this method is a no-op.
   *
   * @param config The config to re-write
   * @return re-written config
   */
  public static Config rewriteConfig(Config config) {
    Optional<String> configRewriterNamesOptional = new JobConfig(config).getConfigRewriters();
    if (configRewriterNamesOptional.isPresent()) {
      String[] configRewriterNames = configRewriterNamesOptional.get().split(",");
      Config rewrittenConfig = config;
      for (String configRewriterName : configRewriterNames) {
        rewrittenConfig = applyRewriter(rewrittenConfig, configRewriterName);
      }
      return rewrittenConfig;
    } else {
      return config;
    }
  }

  /**
   * Re-writes configuration using a ConfigRewriter, defined with the given rewriterName in config.
   * @param config the config to re-write
   * @param rewriterName the name of the rewriter to apply
   * @return the rewritten config
   */
  public static Config applyRewriter(Config config, String rewriterName) {
    String rewriterClassName = new JobConfig(config).getConfigRewriterClass(rewriterName)
        .orElseThrow(() -> new SamzaException(
            String.format("Unable to find class config for config rewriter %s.", rewriterName)));
    ConfigRewriter rewriter = ReflectionUtil.getObj(rewriterClassName, ConfigRewriter.class);
    LOG.info("Re-writing config with {}", rewriter);
    return rewriter.rewrite(rewriterName, config);
  }

  /**
   * Do this so Powermockito can mock the system classes.
   * Powermockito doesn't seem to work as well with Scala singletons.
   * In Java, it seems like it will work to use Powermock without this wrapper.
   */
  static class NetworkingUtil {
    public InetAddress inetAddressGetLocalHost() throws UnknownHostException {
      return InetAddress.getLocalHost();
    }

    public InetAddress inetAddressGetByAddress(byte[] address) throws UnknownHostException {
      return InetAddress.getByAddress(address);
    }

    public String inetAddressGetHostAddress(InetAddress inetAddress) {
      return inetAddress.getHostAddress();
    }

    public byte[] inetAddressGetAddress(InetAddress inetAddress) {
      return inetAddress.getAddress();
    }

    public Enumeration<NetworkInterface> networkInterfaceGetNetworkInterfaces() throws SocketException {
      return NetworkInterface.getNetworkInterfaces();
    }

    public Enumeration<InetAddress> networkInterfaceGetInetAddresses(NetworkInterface networkInterface) {
      return networkInterface.getInetAddresses();
    }
  }
}
