package org.apache.samza.test.timer;

import org.apache.samza.test.operator.StreamApplicationIntegrationTestHarness;
import org.junit.Before;
import org.junit.Test;


import static org.apache.samza.test.timer.TestTimerApp.PAGE_VIEWS;

/**
 * Created by xiliu on 2/1/18.
 */
public class TimerTest extends StreamApplicationIntegrationTestHarness {

  @Before
  public void setup() {
    // create topics
    createTopic(PAGE_VIEWS, 2);

    // create events for the following user activity.
    // userId: (viewId, pageId, (adIds))
    // u1: (v1, p1, (a1)), (v2, p2, (a3))
    // u2: (v3, p1, (a1)), (v4, p3, (a5))
    produceMessage(PAGE_VIEWS, 0, "p1", "{\"viewId\":\"v1\",\"pageId\":\"p1\",\"userId\":\"u1\"}");
    produceMessage(PAGE_VIEWS, 1, "p2", "{\"viewId\":\"v2\",\"pageId\":\"p2\",\"userId\":\"u1\"}");
    produceMessage(PAGE_VIEWS, 0, "p1", "{\"viewId\":\"v3\",\"pageId\":\"p1\",\"userId\":\"u2\"}");
    produceMessage(PAGE_VIEWS, 1, "p3", "{\"viewId\":\"v4\",\"pageId\":\"p3\",\"userId\":\"u2\"}");

  }

  @Test
  public void testJob() {
    runApplication(new TestTimerApp(), "TimerTest", null);
  }
}
