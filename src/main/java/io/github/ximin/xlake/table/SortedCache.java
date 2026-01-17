/*-
 * #%L
 * xlake-demo
 * %%
 * Copyright (C) 2026 ximin1024
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.github.ximin.xlake.table;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SortedCache<K extends Comparable<K>, V> {
    private final TreeMap<K, V> cache = new TreeMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final int capacity;

    public SortedCache(int capacity) {
        this.capacity = capacity;
    }

    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            cache.put(key, value);

            if (cache.size() > capacity) {
                cache.remove(cache.firstKey());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V get(K key) {
        lock.readLock().lock();
        try {
            return cache.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public V remove(K key) {
        lock.writeLock().lock();
        try {
            return cache.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean containsKey(K key) {
        lock.readLock().lock();
        try {
            return cache.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<K, V> getAll() {
        lock.readLock().lock();
        try {
            return new TreeMap<>(cache);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return cache.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}
