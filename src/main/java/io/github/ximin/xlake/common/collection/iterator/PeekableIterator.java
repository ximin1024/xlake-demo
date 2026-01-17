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

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekableIterator<T> implements Iterator<T> {
    private final Iterator<T> iterator;
    private T nextElement;
    private boolean peeked;

    public PeekableIterator(Iterator<T> iterator) {
        this.iterator = iterator;
        advance();
    }

    @Override
    public boolean hasNext() {
        return peeked;
    }

    @Override
    public T next() {
        if (!peeked) {
            throw new NoSuchElementException();
        }
        T result = nextElement;
        advance();
        return result;
    }

    public T peek() {
        if (!peeked) {
            throw new NoSuchElementException();
        }
        return nextElement;
    }

    private void advance() {
        peeked = iterator.hasNext();
        if (peeked) {
            nextElement = iterator.next();
        } else {
            nextElement = null;
        }
    }
}
