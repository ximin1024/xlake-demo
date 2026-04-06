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
package io.github.ximin.xlake.table.schema;

import java.util.HashMap;
import java.util.Map;

public record StructField(String name, XlakeType dataType,
                          String comment, Map<String, String> metadata) {

    public StructField {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("Field name cannot be empty");
        if (dataType == null)
            throw new IllegalArgumentException("Field data type cannot be null");
        comment = comment != null ? comment : "";
        metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
    }

    public StructField(String name, XlakeType dataType) {
        this(name, dataType, "", Map.of());
    }

    public StructField(String name, XlakeType dataType, String comment) {
        this(name, dataType, comment, Map.of());
    }

    public StructField withComment(String newComment) {
        return new StructField(name, dataType, newComment, metadata);
    }

    public StructField withMetadata(String key, String value) {
        Map<String, String> newMetadata = new HashMap<>(metadata);
        newMetadata.put(key, value);
        return new StructField(name, dataType, comment, newMetadata);
    }

    public StructField withPrimaryKey(boolean primaryKey) {
        return new StructField(name, dataType, comment, metadata);
    }
}
