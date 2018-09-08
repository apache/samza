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
  private SystemTimerScheduler systemTimerScheduler;

  private SchedulerImpl scheduler;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    scheduler = new SchedulerImpl(systemTimerScheduler);
  }

  /**
   * scheduleCallback should delegate to the inner scheduler
   */
  @Test
  public void testScheduleCallback() {
    @SuppressWarnings("unchecked")
    TimerCallback<String> stringCallback = mock(TimerCallback.class);
    scheduler.scheduleCallback("string_key", 123, stringCallback);
    verify(systemTimerScheduler).setTimer("string_key", 123, stringCallback);

    // check some other type of key
    @SuppressWarnings("unchecked")
    TimerCallback<Integer> intCallback = mock(TimerCallback.class);
    scheduler.scheduleCallback(777, 456, intCallback);
    verify(systemTimerScheduler).setTimer(777, 456, intCallback);
  }

  /**
   * deleteCallback should delegate to the inner scheduler
   */
  @Test
  public void testDeleteCallback() {
    scheduler.deleteCallback("string_key");
    verify(systemTimerScheduler).deleteTimer("string_key");

    // check some other type of key
    scheduler.deleteCallback(777);
    verify(systemTimerScheduler).deleteTimer(777);
  }
}