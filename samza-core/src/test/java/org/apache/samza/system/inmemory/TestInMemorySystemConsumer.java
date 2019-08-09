package org.apache.samza.system.inmemory;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;


public class TestInMemorySystemConsumer {
  private static final SystemStreamPartition SSP0 = new SystemStreamPartition("system", "stream", new Partition(0));
  private static final SystemStreamPartition SSP1 = new SystemStreamPartition("system", "stream", new Partition(1));

  @Mock
  private InMemoryManager inMemoryManager;

  private InMemorySystemConsumer inMemorySystemConsumer;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.inMemorySystemConsumer = new InMemorySystemConsumer(this.inMemoryManager);
  }

  @Test
  public void testPoll() throws InterruptedException {
    this.inMemorySystemConsumer.register(SSP0, "1");
    this.inMemorySystemConsumer.register(SSP1, "1");

    IncomingMessageEnvelope ime01 = new IncomingMessageEnvelope(SSP0, "1", "key01", "message01");
    IncomingMessageEnvelope ime02 = new IncomingMessageEnvelope(SSP0, "2", "key02", "message02");
    Map<SystemStreamPartition, String> pollRequest = ImmutableMap.of(SSP0, "1");
    when(this.inMemoryManager.poll(pollRequest))
        // poll for SSP0 only, return no messages
        .thenReturn(ImmutableMap.of(SSP0, ImmutableList.of()))
        // poll for SSP0 only, return some messages; still same offset request since got no messages last time
        .thenReturn(ImmutableMap.of(SSP0, ImmutableList.of(ime01, ime02)));
    // poll for SSP0 and SSP1; SSP0 should have a new offset now
    pollRequest = ImmutableMap.of(SSP0, "3", SSP1, "1");
    IncomingMessageEnvelope ime03 = new IncomingMessageEnvelope(SSP0, "3", "key03", "message03");
    IncomingMessageEnvelope ime10 = new IncomingMessageEnvelope(SSP1, "1", "key10", "message10");
    when(this.inMemoryManager.poll(pollRequest)).thenReturn(
        ImmutableMap.of(SSP0, ImmutableList.of(ime03), SSP1, ImmutableList.of(ime10)));

    assertEquals(ImmutableMap.of(SSP0, ImmutableList.of()),
        this.inMemorySystemConsumer.poll(ImmutableSet.of(SSP0), 1000));
    assertEquals(ImmutableMap.of(SSP0, ImmutableList.of(ime01, ime02)),
        this.inMemorySystemConsumer.poll(ImmutableSet.of(SSP0), 1000));
    assertEquals(ImmutableMap.of(SSP0, ImmutableList.of(ime03), SSP1, ImmutableList.of(ime10)),
        this.inMemorySystemConsumer.poll(ImmutableSet.of(SSP0, SSP1), 1000));
  }

  @Test
  public void testPollRegisterNullOffset() throws InterruptedException {
    this.inMemorySystemConsumer.register(SSP0, null);

    IncomingMessageEnvelope ime0 = new IncomingMessageEnvelope(SSP0, "0", "key0", "message0");
    IncomingMessageEnvelope ime1 = new IncomingMessageEnvelope(SSP0, "1", "key1", "message1");
    Map<SystemStreamPartition, String> pollRequest = ImmutableMap.of(SSP0, "0");
    when(this.inMemoryManager.poll(pollRequest)).thenReturn(ImmutableMap.of(SSP0, ImmutableList.of(ime0)));
    pollRequest = ImmutableMap.of(SSP0, "1");
    when(this.inMemoryManager.poll(pollRequest)).thenReturn(ImmutableMap.of(SSP0, ImmutableList.of(ime1)));

    assertEquals(ImmutableMap.of(SSP0, ImmutableList.of(ime0)),
        this.inMemorySystemConsumer.poll(ImmutableSet.of(SSP0), 1000));
    assertEquals(ImmutableMap.of(SSP0, ImmutableList.of(ime1)),
        this.inMemorySystemConsumer.poll(ImmutableSet.of(SSP0), 1000));
  }
}