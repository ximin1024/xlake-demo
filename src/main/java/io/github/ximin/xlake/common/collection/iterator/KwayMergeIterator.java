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
package io.github.ximin.xlake.common.collection.iterator;

import java.util.*;

public class KwayMergeIterator<T> implements Iterator<T> {
    private final PriorityQueue<PeekableIterator<T>> heap;

    public KwayMergeIterator(List<Iterator<T>> iterators, Comparator<T> comparator) {
        this.heap = new PriorityQueue<>(iterators.size(),
                Comparator.comparing(PeekableIterator::peek, comparator));
        for (Iterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                heap.add(new PeekableIterator<>(iterator));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        PeekableIterator<T> current = heap.poll();
        T value = current.next();

        if (current.hasNext()) {
            heap.add(current);
        }

        return value;
    }
}
