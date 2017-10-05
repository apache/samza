package org.apache.samza.operators.impl;

import com.google.common.primitives.UnsignedBytes;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class InternalInMemoryStore<K, V> implements KeyValueStore<K, V> {

    private final ConcurrentSkipListMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public InternalInMemoryStore(Serde<K> keySerde, Serde<V> valSerde) {
        this.keySerde = keySerde;
        this.valSerde = valSerde;
    }

    @Override
    public V get(K key) {
        byte[] keyBytes = keySerde.toBytes(key);
        byte[] valBytes = map.get(keyBytes);
        return valSerde.fromBytes(valBytes);
    }

    @Override
    public Map<K, V> getAll(List<K> keys) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void put(K key, V value) {
        map.put(keySerde.toBytes(key), valSerde.toBytes(value));
    }

    @Override
    public void putAll(List<Entry<K, V>> entries) {
        for (Entry<K, V> entry : entries) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(K key) {
        map.remove(keySerde.toBytes(key));
    }

    @Override
    public void deleteAll(List<K> keys) {
        for (K k : keys) {
            delete(k);
        }
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return new InMemoryIterator(map.subMap(keySerde.toBytes(from), keySerde.toBytes(to)).entrySet().iterator(), keySerde, valSerde);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new InMemoryIterator(map.entrySet().iterator(), keySerde, valSerde);
    }

    @Override
    public void close() {

    }

    @Override
    public void flush() {

    }

    private static class InMemoryIterator<K, V> implements KeyValueIterator<K, V> {

        Iterator<Map.Entry<byte[], byte[]>> wrapped;
        Serde<K> keySerializer;
        Serde<V> valSerializer;

        public InMemoryIterator(Iterator<Map.Entry<byte[], byte[]>> wrapped, Serde<K> keySerde, Serde<V> valSerde) {
            this.wrapped = wrapped;
            this.keySerializer = keySerde;
            this.valSerializer = valSerde;
        }

        @Override
        public void close() {
            //no op
        }

        @Override
        public boolean hasNext() {
            return wrapped.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            Map.Entry<byte[], byte[]> next = wrapped.next();
            return new Entry<>(keySerializer.fromBytes(next.getKey()), valSerializer.fromBytes(next.getValue()));
        }
    }
}
