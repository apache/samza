package org.apache.samza.system.inmemory;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;


public class TestInMemorySystemAdmin {
  @Mock
  private InMemoryManager inMemoryManager;

  private InMemorySystemAdmin inMemorySystemAdmin;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.inMemorySystemAdmin = new InMemorySystemAdmin("system", this.inMemoryManager);
  }

  @Test
  public void testOffsetComparator() {
    assertEquals(0, inMemorySystemAdmin.offsetComparator(null, null).intValue());
    assertEquals(-1, inMemorySystemAdmin.offsetComparator(null, "0").intValue());
    assertEquals(1, inMemorySystemAdmin.offsetComparator("0", null).intValue());
    assertEquals(-1, inMemorySystemAdmin.offsetComparator("0", "1").intValue());
    assertEquals(0, inMemorySystemAdmin.offsetComparator("0", "0").intValue());
    assertEquals(1, inMemorySystemAdmin.offsetComparator("1", "0").intValue());
  }
}