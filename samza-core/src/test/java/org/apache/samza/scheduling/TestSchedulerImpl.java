package org.apache.samza.scheduling;

import org.apache.samza.task.SystemTimerScheduler;
import org.apache.samza.task.TimerCallback;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;


public class TestSchedulerImpl {
  @Mock
  private SystemTimerScheduler _systemTimerScheduler;

  private SchedulerImpl _scheduler;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    _scheduler = new SchedulerImpl(_systemTimerScheduler);
  }

  /**
   * scheduleCallback should delegate to the inner scheduler
   */
  @Test
  public void testScheduleCallback() {
    @SuppressWarnings("unchecked")
    TimerCallback<String> stringCallback = mock(TimerCallback.class);
    _scheduler.scheduleCallback("string_key", 123, stringCallback);
    verify(_systemTimerScheduler).setTimer("string_key", 123, stringCallback);

    // check some other type of key
    @SuppressWarnings("unchecked")
    TimerCallback<Integer> intCallback = mock(TimerCallback.class);
    _scheduler.scheduleCallback(777, 456, intCallback);
    verify(_systemTimerScheduler).setTimer(777, 456, intCallback);
  }

  /**
   * deleteCallback should delegate to the inner scheduler
   */
  @Test
  public void testDeleteCallback() {
    _scheduler.deleteCallback("string_key");
    verify(_systemTimerScheduler).deleteTimer("string_key");

    // check some other type of key
    _scheduler.deleteCallback(777);
    verify(_systemTimerScheduler).deleteTimer(777);
  }
}