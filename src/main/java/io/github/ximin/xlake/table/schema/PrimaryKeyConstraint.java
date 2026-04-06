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

import java.util.List;

public record PrimaryKeyConstraint(String name, List<String> fieldNames, boolean enforced,
                                   String description) implements Constraint {

    public PrimaryKeyConstraint {
        if (fieldNames == null || fieldNames.isEmpty()) {
            throw new IllegalArgumentException("Primary key must have at least one field");
        }
        if (name == null || name.trim().isEmpty()) {
            name = "PK_" + String.join("_", fieldNames);
        }
        fieldNames = List.copyOf(fieldNames);
        description = description != null ? description :
                String.format("Primary key constraint on %s", fieldNames);
    }

    public PrimaryKeyConstraint(List<String> fieldNames) {
        this(null, fieldNames, true, null);
    }

    public PrimaryKeyConstraint(String name, List<String> fieldNames) {
        this(name, fieldNames, true, null);
    }

    public boolean isComposite() {
        return fieldNames.size() > 1;
    }
}
