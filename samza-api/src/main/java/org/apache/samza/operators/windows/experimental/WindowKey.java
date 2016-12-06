
package org.apache.samza.operators.windows.experimental;

public class WindowKey<K> {
  private final long windowStart;
  private final  long windowEnd;
  private final  K key;

  WindowKey(K key, long windowStart, long windowEnd) {
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.key = key;
  }

  public long getWindowStart() {
    return windowStart;
  }

  public long getWindowEnd() {
    return windowEnd;
  }

  public K getKey() {
    return key;
  }
}

