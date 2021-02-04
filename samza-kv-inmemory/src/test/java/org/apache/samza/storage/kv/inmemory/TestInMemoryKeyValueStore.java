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

package org.apache.samza.storage.kv.inmemory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueSnapshot;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


public class TestInMemoryKeyValueStore {
  private static final String DEFAULT_KEY_PREFIX = "key_prefix";
  private static final String OTHER_KEY_PREFIX = "other_key_prefix";
  /**
   * Keep the lengths of the values longer so that metrics validations for key and value sizes don't collide.
   */
  private static final String DEFAULT_VALUE_PREFIX = "value_prefix_value_prefix";
  private static final String OTHER_VALUE_PREFIX = "other_value_prefix_value_prefix";

  /**
   * The length of the result of {@link #key(int)} will always be the same, so this can be used as the length for any
   * key produced by {@link #key(int)}.
   */
  private static final int DEFAULT_KEY_LENGTH = key(0).length;
  /**
   * The length of the result of {@link #value(int)} will always be the same, so this can be used as the length for any
   * key produced by {@link #value(int)}.
   */
  private static final int DEFAULT_VALUE_LENGTH = value(0).length;

  @Mock
  private KeyValueStoreMetrics keyValueStoreMetrics;
  @Mock
  private Counter getsCounter;
  @Mock
  private Counter bytesReadCounter;
  @Mock
  private Counter putsCounter;
  @Mock
  private Counter bytesWrittenCounter;
  @Mock
  private Counter deletesCounter;

  private InMemoryKeyValueStore inMemoryKeyValueStore;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(this.keyValueStoreMetrics.gets()).thenReturn(this.getsCounter);
    when(this.keyValueStoreMetrics.bytesRead()).thenReturn(this.bytesReadCounter);
    when(this.keyValueStoreMetrics.puts()).thenReturn(this.putsCounter);
    when(this.keyValueStoreMetrics.bytesWritten()).thenReturn(this.bytesWrittenCounter);
    when(this.keyValueStoreMetrics.deletes()).thenReturn(this.deletesCounter);
    this.inMemoryKeyValueStore = new InMemoryKeyValueStore(this.keyValueStoreMetrics);
  }

  @Test
  public void testGet() {
    this.inMemoryKeyValueStore.put(key(0), value(0));
    this.inMemoryKeyValueStore.put(key(OTHER_KEY_PREFIX, 1), value(OTHER_VALUE_PREFIX, 1));

    assertArrayEquals(value(0), this.inMemoryKeyValueStore.get(key(0)));
    assertArrayEquals(value(OTHER_VALUE_PREFIX, 1), this.inMemoryKeyValueStore.get(key(OTHER_KEY_PREFIX, 1)));
    verify(this.getsCounter, times(2)).inc();
    verify(this.bytesReadCounter).inc(DEFAULT_VALUE_LENGTH);
    verify(this.bytesReadCounter).inc(value(OTHER_VALUE_PREFIX, 1).length);
  }

  @Test
  public void testGetEmpty() {
    assertNull(this.inMemoryKeyValueStore.get(key(0)));
    verify(this.getsCounter).inc();
    verifyZeroInteractions(this.bytesReadCounter);
  }

  @Test
  public void testGetAfterDelete() {
    this.inMemoryKeyValueStore.put(key(0), value(0));
    this.inMemoryKeyValueStore.delete(key(0));

    assertNull(this.inMemoryKeyValueStore.get(key(0)));
    verify(this.getsCounter).inc();
    verifyZeroInteractions(this.bytesReadCounter);
  }

  @Test
  public void testPut() {
    this.inMemoryKeyValueStore.put(key(0), value(0));
    this.inMemoryKeyValueStore.put(key(OTHER_KEY_PREFIX, 1), value(OTHER_VALUE_PREFIX, 1));

    assertArrayEquals(value(0), this.inMemoryKeyValueStore.get(key(0)));
    assertArrayEquals(value(OTHER_VALUE_PREFIX, 1), this.inMemoryKeyValueStore.get(key(OTHER_KEY_PREFIX, 1)));
    verify(this.putsCounter, times(2)).inc();
    verify(this.bytesWrittenCounter).inc(DEFAULT_KEY_LENGTH + DEFAULT_VALUE_LENGTH);
    verify(this.bytesWrittenCounter).inc(key(OTHER_KEY_PREFIX, 1).length + value(OTHER_VALUE_PREFIX, 1).length);
  }

  @Test
  public void testPutExistingEntry() {
    this.inMemoryKeyValueStore.put(key(0), value(0));
    this.inMemoryKeyValueStore.put(key(0), value(OTHER_VALUE_PREFIX, 1));

    assertArrayEquals(value(OTHER_VALUE_PREFIX, 1), this.inMemoryKeyValueStore.get(key(0)));
    verify(this.putsCounter, times(2)).inc();
    verify(this.bytesWrittenCounter).inc(DEFAULT_KEY_LENGTH + DEFAULT_VALUE_LENGTH);
    verify(this.bytesWrittenCounter).inc(DEFAULT_KEY_LENGTH + value(OTHER_VALUE_PREFIX, 1).length);
  }

  @Test
  public void testPutEmptyValue() {
    byte[] emptyValue = new byte[0];
    this.inMemoryKeyValueStore.put(key(0), emptyValue);

    assertEquals(0, this.inMemoryKeyValueStore.get(key(0)).length);
    verify(this.putsCounter).inc();
    verify(this.bytesWrittenCounter).inc(DEFAULT_KEY_LENGTH);
  }

  @Test
  public void testPutNull() {
    this.inMemoryKeyValueStore.put(key(0), value(0));
    this.inMemoryKeyValueStore.put(key(0), null);

    assertNull(this.inMemoryKeyValueStore.get(key(0)));
    verify(this.putsCounter, times(2)).inc();
    verify(this.deletesCounter).inc();
    verify(this.bytesWrittenCounter).inc(DEFAULT_KEY_LENGTH + DEFAULT_VALUE_LENGTH);
  }

  @Test
  public void testPutAll() {
    List<Entry<byte[], byte[]>> entries = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      entries.add(new Entry<>(key(i), value(i)));
    }
    this.inMemoryKeyValueStore.putAll(entries);

    for (int i = 0; i < 10; i++) {
      assertArrayEquals(value(i), this.inMemoryKeyValueStore.get(key(i)));
    }
    verify(this.putsCounter, times(10)).inc();
    // when using key(i) and value(i), the byte[] lengths will be the same
    verify(this.bytesWrittenCounter, times(10)).inc(DEFAULT_KEY_LENGTH + DEFAULT_VALUE_LENGTH);
  }

  @Test
  public void testPutAllUpdate() {
    // check that an existing value is overridden
    this.inMemoryKeyValueStore.put(key(0), value(OTHER_VALUE_PREFIX, 0));
    List<Entry<byte[], byte[]>> entries = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      entries.add(new Entry<>(key(i), value(i)));
    }
    this.inMemoryKeyValueStore.putAll(entries);

    for (int i = 0; i < 10; i++) {
      assertArrayEquals(value(i), this.inMemoryKeyValueStore.get(key(i)));
    }
    // 1 time for initial value to be overridden, 10 times for "regular" puts
    verify(this.putsCounter, times(11)).inc();
    // for initial value which is overridden
    verify(this.bytesWrittenCounter).inc(DEFAULT_KEY_LENGTH + value(OTHER_VALUE_PREFIX, 0).length);
    // when using key(i) and value(i), the byte[] lengths will be the same
    verify(this.bytesWrittenCounter, times(10)).inc(DEFAULT_KEY_LENGTH + DEFAULT_VALUE_LENGTH);
  }

  @Test
  public void testPutAllWithNull() {
    List<Entry<byte[], byte[]>> entries = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      entries.add(new Entry<>(key(i), value(i)));
    }
    this.inMemoryKeyValueStore.putAll(entries);

    List<Entry<byte[], byte[]>> deleteEntries = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      deleteEntries.add(new Entry<>(key(i), null));
    }
    this.inMemoryKeyValueStore.putAll(deleteEntries);

    for (int i = 0; i < 10; i++) {
      if (i < 3) {
        assertNull(this.inMemoryKeyValueStore.get(key(i)));
      } else {
        assertArrayEquals(value(i), this.inMemoryKeyValueStore.get(key(i)));
      }
    }
    // 10 times for "regular" puts, 3 times for deletion puts
    verify(this.putsCounter, times(13)).inc();
    // 10 "regular" puts all have same size for key/value
    verify(this.bytesWrittenCounter, times(10)).inc(DEFAULT_KEY_LENGTH + DEFAULT_VALUE_LENGTH);
    verifyNoMoreInteractions(this.bytesWrittenCounter);
    verify(this.deletesCounter, times(3)).inc();
  }

  @Test
  public void testDelete() {
    this.inMemoryKeyValueStore.put(key(0), value(0));
    this.inMemoryKeyValueStore.delete(key(0));
    assertNull(this.inMemoryKeyValueStore.get(key(0)));

    verify(this.deletesCounter, times(1)).inc();
  }

  @Test
  public void testDeleteNonExistentEntry() {
    this.inMemoryKeyValueStore.delete(key(0));

    assertNull(this.inMemoryKeyValueStore.get(key(0)));

    verify(this.deletesCounter, times(1)).inc();
  }

  @Test
  public void testRange() {
    Counter rangesCounter = mock(Counter.class);
    when(this.keyValueStoreMetrics.ranges()).thenReturn(rangesCounter);

    for (int i = 0; i < 10; i++) {
      this.inMemoryKeyValueStore.put(key(OTHER_KEY_PREFIX, i), value(OTHER_VALUE_PREFIX, i));
      this.inMemoryKeyValueStore.put(key(i), value(i));
    }
    KeyValueIterator<byte[], byte[]> range = this.inMemoryKeyValueStore.range(key(0), key(5));

    for (int i = 0; i < 5; i++) {
      assertEntryEquals(key(i), value(i), range.next());
    }
    assertFalse(range.hasNext());
    verify(rangesCounter).inc();
    // key size increments: key(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(5)).inc(DEFAULT_KEY_LENGTH);
    // value size increments: value(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(5)).inc(DEFAULT_VALUE_LENGTH);
  }

  @Test
  public void testRangeWithUpdate() {
    Counter rangesCounter = mock(Counter.class);
    when(this.keyValueStoreMetrics.ranges()).thenReturn(rangesCounter);

    // exclude the index 1 entry so it can be added later
    for (int i = 0; i < 10; i++) {
      if (i != 1) {
        this.inMemoryKeyValueStore.put(key(i), value(i));
      }
    }
    KeyValueIterator<byte[], byte[]> range = this.inMemoryKeyValueStore.range(key(0), key(5));
    assertEquals(4, Iterators.size(range));

    this.inMemoryKeyValueStore.delete(key(2)); // delete an entry from the range
    this.inMemoryKeyValueStore.put(key(3), value(OTHER_VALUE_PREFIX, 3)); // update an entry
    this.inMemoryKeyValueStore.put(key(1), value(DEFAULT_VALUE_PREFIX, 1)); // add a new entry
    range = this.inMemoryKeyValueStore.range(key(0), key(5));
    for (int i = 0; i < 5; i++) {
      if (i != 2) { // index 2 was deleted
        if (i == 3) { // index 3 has an updated value
          assertEntryEquals(key(i), value(OTHER_VALUE_PREFIX, 3), range.next());
        } else {
          // all other entries (including index 1 have the "normal" entries)
          assertEntryEquals(key(i), value(DEFAULT_VALUE_PREFIX, i), range.next());
        }
      }
    }
    assertFalse(range.hasNext());

    verify(rangesCounter, times(2)).inc();
    // key increments: 4 for iterator size for first range, 4 for second range; key(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(8)).inc(DEFAULT_KEY_LENGTH);
    /*
     * value increments: 4 for iterator size for first range, 3 for second range (updated entry is different); value(i)
     * produces byte[] of same length
     */
    verify(this.bytesReadCounter, times(7)).inc(DEFAULT_VALUE_LENGTH);
    // 1 call for updated entry
    verify(this.bytesReadCounter).inc(value(OTHER_VALUE_PREFIX, 0).length);
  }

  @Test
  public void testSnapshot() {
    for (int i = 0; i < 10; i++) {
      this.inMemoryKeyValueStore.put(key(OTHER_KEY_PREFIX, i), value(OTHER_VALUE_PREFIX, i));
      this.inMemoryKeyValueStore.put(key(i), value(i));
    }
    KeyValueSnapshot<byte[], byte[]> snapshot = this.inMemoryKeyValueStore.snapshot(key(0), key(5));
    KeyValueIterator<byte[], byte[]> iterator = snapshot.iterator();

    for (int i = 0; i < 5; i++) {
      assertEntryEquals(key(i), value(i), iterator.next());
    }
    assertFalse(iterator.hasNext());
    // key size increments: key(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(5)).inc(DEFAULT_KEY_LENGTH);
    // value size increments: value(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(5)).inc(DEFAULT_VALUE_LENGTH);
  }

  @Test
  public void testSnapshotMultipleIterators() {
    for (int i = 0; i < 10; i++) {
      this.inMemoryKeyValueStore.put(key(OTHER_KEY_PREFIX, i), value(OTHER_VALUE_PREFIX, i));
      this.inMemoryKeyValueStore.put(key(i), value(i));
    }
    KeyValueSnapshot<byte[], byte[]> snapshot = this.inMemoryKeyValueStore.snapshot(key(0), key(5));
    assertEquals(5, Iterators.size(snapshot.iterator())); // Iterators.size exhausts the iterator
    assertEquals(5, Iterators.size(snapshot.iterator()));
    // key size increments: calling two separate iterators; key(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(5 * 2)).inc(DEFAULT_KEY_LENGTH);
    // value size increments: calling two separate iterators; value(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(5 * 2)).inc(DEFAULT_VALUE_LENGTH);
  }

  @Test
  public void testSnapshotImmutable() {
    for (int i = 0; i < 10; i++) {
      this.inMemoryKeyValueStore.put(key(i), value(i));
    }
    KeyValueSnapshot<byte[], byte[]> snapshot = this.inMemoryKeyValueStore.snapshot(key(0), key(5));
    // make sure the entries in the snapshot don't change when something is added
    this.inMemoryKeyValueStore.put(key(1), value(OTHER_VALUE_PREFIX, 1));
    KeyValueIterator<byte[], byte[]> iterator = snapshot.iterator();

    for (int i = 0; i < 5; i++) {
      if (i == 1) {
        /*
         * There is a bug in which the snapshot is impacted by writes after calling snapshot.
         * When the bug is fixed, the value for key(1) should be the original value from when snapshot was called.
         */
        assertEntryEquals(key(i), value(OTHER_VALUE_PREFIX, 1), iterator.next());
      } else {
        assertEntryEquals(key(i), value(i), iterator.next());
      }
    }
    assertFalse(iterator.hasNext());
    // key size increments; key(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(5)).inc(DEFAULT_KEY_LENGTH);
    // value size increments; value(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(4)).inc(DEFAULT_VALUE_LENGTH);
    // when snapshot immutability bug is fixed, this should be merged into the bytesRead check above
    verify(this.bytesReadCounter).inc(value(OTHER_VALUE_PREFIX, 1).length);
  }

  @Test
  public void testSnapshotWithUpdate() {
    // exclude the index 1 entry so it can be added later
    for (int i = 0; i < 10; i++) {
      if (i != 1) {
        this.inMemoryKeyValueStore.put(key(i), value(i));
      }
    }
    KeyValueSnapshot<byte[], byte[]> snapshot = this.inMemoryKeyValueStore.snapshot(key(0), key(5));
    assertEquals(4, Iterators.size(snapshot.iterator()));

    this.inMemoryKeyValueStore.delete(key(2)); // delete an entry fro the snapshot range
    this.inMemoryKeyValueStore.put(key(3), value(OTHER_VALUE_PREFIX, 3)); // update an entry
    this.inMemoryKeyValueStore.put(key(1), value(DEFAULT_VALUE_PREFIX, 1)); // add a new entry
    snapshot = this.inMemoryKeyValueStore.snapshot(key(0), key(5));
    KeyValueIterator<byte[], byte[]> iterator = snapshot.iterator();
    for (int i = 0; i < 5; i++) {
      if (i != 2) { // index 2 was deleted
        if (i == 3) { // index 3 has an updated value
          assertEntryEquals(key(i), value(OTHER_VALUE_PREFIX, 3), iterator.next());
        } else {
          // all other entries (including index 1 have the "normal" entries)
          assertEntryEquals(key(i), value(DEFAULT_VALUE_PREFIX, i), iterator.next());
        }
      }
    }
    assertFalse(iterator.hasNext());

    // key increments: 4 for iterator size for first range, 4 for second range; key(i) produces byte[] of same length
    verify(this.bytesReadCounter, times(8)).inc(DEFAULT_KEY_LENGTH);
    /*
     * value increments: 4 for iterator size for first range, 3 for second range (updated entry is different); value(i)
     * produces byte[] of same length
     */
    verify(this.bytesReadCounter, times(7)).inc(DEFAULT_VALUE_LENGTH);
    // 1 call for updated entry
    verify(this.bytesReadCounter).inc(value(OTHER_VALUE_PREFIX, 0).length);
  }

  @Test
  public void testAll() {
    Counter allsCounter = mock(Counter.class);
    when(this.keyValueStoreMetrics.alls()).thenReturn(allsCounter);

    for (int i = 0; i < 10; i++) {
      this.inMemoryKeyValueStore.put(key(OTHER_KEY_PREFIX, i), value(OTHER_VALUE_PREFIX, i));
      this.inMemoryKeyValueStore.put(key(i), value(i));
    }
    KeyValueIterator<byte[], byte[]> all = this.inMemoryKeyValueStore.all();

    // all entries of one prefix comes first due to ordering
    for (int i = 0; i < 10; i++) {
      assertEntryEquals(key(i), value(i), all.next());
    }
    for (int i = 0; i < 10; i++) {
      assertEntryEquals(key(OTHER_KEY_PREFIX, i), value(OTHER_VALUE_PREFIX, i), all.next());
    }
    assertFalse(all.hasNext());

    verify(allsCounter).inc();

    // key size increments: 10 calls for each prefix
    verify(this.bytesReadCounter, times(10)).inc(DEFAULT_KEY_LENGTH);
    // all keys using OTHER_KEY_PREFIX have the same length
    int otherKeyLength = key(OTHER_KEY_PREFIX, 0).length;
    verify(this.bytesReadCounter, times(10)).inc(otherKeyLength);

    // value size increments: 10 calls for each prefix
    verify(this.bytesReadCounter, times(10)).inc(DEFAULT_VALUE_LENGTH);
    // all values using OTHER_VALUE_PREFIX have the same length
    int otherValueLength = value(OTHER_VALUE_PREFIX, 0).length;
    verify(this.bytesReadCounter, times(10)).inc(otherValueLength);
  }

  @Test
  public void testAllWithUpdate() {
    Counter allsCounter = mock(Counter.class);
    when(this.keyValueStoreMetrics.alls()).thenReturn(allsCounter);

    // fill in a range values for two different prefixes, but leave out index 1 so it can be added later
    for (int i = 0; i < 10; i++) {
      if (i != 1) {
        this.inMemoryKeyValueStore.put(key(i), value(i));
      }
    }
    KeyValueIterator<byte[], byte[]> all = this.inMemoryKeyValueStore.all();
    assertEquals(9, Iterators.size(all));

    this.inMemoryKeyValueStore.delete(key(2)); // delete an entry
    this.inMemoryKeyValueStore.put(key(3), value(OTHER_VALUE_PREFIX, 3)); // update an entry
    this.inMemoryKeyValueStore.put(key(1), value(1)); // add a new entry
    all = this.inMemoryKeyValueStore.all();
    for (int i = 0; i < 10; i++) {
      if (i != 2) { // index 2 was deleted
        if (i == 3) { // index 3 has an updated value
          assertEntryEquals(key(i), value(OTHER_VALUE_PREFIX, 3), all.next());
        } else {
          // all other entries (including index 1 have the "normal" entries)
          assertEntryEquals(key(i), value(i), all.next());
        }
      }
    }
    assertFalse(all.hasNext());

    verify(allsCounter, times(2)).inc();
    // key size increments: 9 calls for iterator size check of first "all", 9 calls for second "all"
    verify(this.bytesReadCounter, times(18)).inc(DEFAULT_KEY_LENGTH);
    /*
     * value size increments: 9 calls for iterator size check of first "all", 8 calls for second "all" (updated entry is
     * different)
     */
    verify(this.bytesReadCounter, times(17)).inc(DEFAULT_VALUE_LENGTH);
    // 1 call for "updatedValue"
    verify(this.bytesReadCounter).inc(value(OTHER_VALUE_PREFIX, 3).length);
  }

  @Test
  public void testFlush() {
    Counter flushesCounter = mock(Counter.class);
    when(this.keyValueStoreMetrics.flushes()).thenReturn(flushesCounter);
    this.inMemoryKeyValueStore.flush();
    verify(flushesCounter).inc();
  }

  /**
   * If this is called multiple times with the same {@code prefix} and any {@code i}, then this needs to return a byte[]
   * of the same length for each call.
   */
  private static byte[] key(String prefix, int i) {
    return toBytes(prefix, i);
  }

  /**
   * The tests depend on the fact that this returns the same length byte[] for any i (for checking metrics).
   */
  private static byte[] key(int i) {
    return key(DEFAULT_KEY_PREFIX, i);
  }

  /**
   * If this is called multiple times with the same {@code prefix} and any {@code i}, then this needs to return a byte[]
   * of the same length for each call.
   */
  private static byte[] value(String prefix, int i) {
    return toBytes(prefix, i);
  }

  /**
   * The tests depend on the fact that this returns the same length byte[] for any i (for checking metrics).
   */
  private static byte[] value(int i) {
    return value(DEFAULT_VALUE_PREFIX, i);
  }

  /**
   * Concatenates bytes for {@code prefix} with bytes for {@code i}.
   * If this is called multiple times with the same {@code prefix} and any {@code i}, then this needs to return a byte[]
   * of the same length for each call.
   */
  private static byte[] toBytes(String prefix, int i) {
    // wrapping with try-catch to avoid dealing with checked exceptions
    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      byteArrayOutputStream.write(prefix.getBytes());
      byteArrayOutputStream.write(Ints.toByteArray(i));
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new SamzaException(e);
    }
  }

  private static void assertEntryEquals(byte[] expectedKey, byte[] expectedValue, Entry<byte[], byte[]> entry) {
    assertArrayEquals(expectedKey, entry.getKey());
    assertArrayEquals(expectedValue, entry.getValue());
  }
}