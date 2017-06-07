package org.apache.samza.zk;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestZkNamespace {
  private static EmbeddedZookeeper zkServer = null;
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private ZkClient zkClient = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;
  private ZkUtils zkUtils;

  @BeforeClass
  public static void setup()
      throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
  }

  @Before
  public void testSetup() {

  }

  @After
  public void testTeardown() {

  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  // for these tests we need to connect to zk multiple times
  private void initZk(String zkConnect) {
    try {
      zkClient = new ZkClient(new ZkConnection(zkConnect, SESSION_TIMEOUT_MS), CONNECTION_TIMEOUT_MS);
    } catch (Exception e) {
      Assert.fail("Client connection setup failed. Aborting tests..");
    }
  }

  private void tearDownZk() {
    zkClient.close();
  }

  private void testCreateZkNameSpace(String zkNameSpace) {
    String zkConnect = "127.0.0.1:" + zkServer.getPort() + zkNameSpace;
    initZk(zkConnect);
    ZkCoordinationServiceFactory.createZkNameSpace(zkConnect, zkClient);

    zkClient.createPersistent("/test");
    zkClient.createPersistent("/test/test1");
    // test if the new root exists
    Assert.assertTrue(zkClient.exists("/"));
    Assert.assertTrue(zkClient.exists("/test"));
    Assert.assertTrue(zkClient.exists("/test/test1"));
    tearDownZk();
  }

  @Test
  public void testCreateZkNameSpace() {
    testCreateZkNameSpace("/zkNameSpace");

    testCreateZkNameSpace("");

    testCreateZkNameSpace("/zkNameSpace/xyz");
  }
}

