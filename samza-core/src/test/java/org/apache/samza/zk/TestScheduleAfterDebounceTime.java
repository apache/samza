package org.apache.samza.zk;

import org.apache.samza.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestScheduleAfterDebounceTime {
  private static final long DEBOUNCE_TIME = 1000;
  int i = 0;
  @Before
  public void setup() {

  }

  class TestObj {
    public void inc() {
      i++;
    }
    public void setTo(int val) {
      i=val;
    }
    public void doNothing() {

    }
  }
  @Test
  public void testSchedule() {
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();

    final TestObj testObj = new TestScheduleAfterDebounceTime.TestObj();
    debounceTimer.scheduleAfterDebounceTime("TEST1", DEBOUNCE_TIME, () -> {
      testObj.inc();
    });
    Assert.assertEquals(0, i);
    try { Thread.sleep(DEBOUNCE_TIME + 10); } catch (InterruptedException e) {Assert.fail("Sleep was interrupted");}
    Assert.assertEquals(1, i);
  }

  @Test
  public void testCancelAndSchedule() {
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();

    final TestObj testObj = new TestScheduleAfterDebounceTime.TestObj();
    debounceTimer.scheduleAfterDebounceTime("TEST1", DEBOUNCE_TIME, () -> {
      testObj.inc();
    });
    Assert.assertEquals(0, i);
    // next schedule should cancel the previous one with the same name
    debounceTimer.scheduleAfterDebounceTime("TEST1", DEBOUNCE_TIME, () -> {
      testObj.setTo(100);
    });

    Assert.assertEquals(0, i);

    // this schedule should not cancel the previous one
    debounceTimer.scheduleAfterDebounceTime("TEST2", DEBOUNCE_TIME, () -> {
      testObj.doNothing();
    });

    try { Thread.sleep(2*DEBOUNCE_TIME + 10); } catch (InterruptedException e) {Assert.fail("Sleep was interrupted");}
    Assert.assertEquals(100, i);
  }
}
