package org.apache.samza.processors;

import java.util.Collection;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;


/**
 * A processor for side input store which consumes from the side input streams and populates/updates the underlying
 * store.
 */
public interface SideInputProcessor {

  /**
   *
   * @param messageEnvelope incoming message envelope
   * @param store key value store associated with the incoming message envelope
   *
   * @return a
   */
  Collection<Entry<?, ?>> process(IncomingMessageEnvelope messageEnvelope, KeyValueStore store);
}
