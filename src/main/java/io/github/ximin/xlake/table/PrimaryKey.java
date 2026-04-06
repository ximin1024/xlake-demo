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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public record PrimaryKey(List<String> fields) implements Serializable {

    public PrimaryKey {
        if (fields != null) {
            fields = List.copyOf(fields);
        } else {
            fields = List.of();
        }
    }

    public static PrimaryKey of(String... fieldNames) {
        return new PrimaryKey(List.of(fieldNames));
    }

    public static PrimaryKey of(List<String> fieldNames) {
        return new PrimaryKey(fieldNames);
    }

    public static PrimaryKey none() {
        return new PrimaryKey(List.of());
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }

    public int size() {
        return fields.size();
    }

    public boolean contains(String fieldName) {
        return fields.contains(fieldName);
    }

    public List<String> fields() {
        return Collections.unmodifiableList(fields);
    }

    @Override
    public String toString() {
        return "PK" + fields;
    }
}
