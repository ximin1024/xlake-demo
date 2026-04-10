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
package io.github.ximin.xlake.table.record;

// todo 待完善，record并不会只是一个kv，得表达多个field、field value，缺乏具体实现
public interface RecordView {

    byte[] key();

    byte[] value();

    boolean hasField(String fieldName);

    Object field(String fieldName);

    default Comparable comparableField(String fieldName) {
        Object value = field(fieldName);
        return value instanceof Comparable<?> comparable ? (Comparable) comparable : null;
    }

    default boolean isNull(String fieldName) {
        return field(fieldName) == null;
    }
}
