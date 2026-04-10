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
package io.github.ximin.xlake.table.op;

public enum OpType {
    FIND("FIND"),
    KV_SCAN("KV_SCAN"),
    BATCH_SCAN("BATCH_SCAN"),
    KV_WRITE("KV_WRITE"),
    APPEND_ONLY_WRITE("APPEND_ONLY_WRITE"),
    INSERT("INSERT"),
    OVERWRITE("OVERWRITE"),
    UPDATE("UPDATE"),
    DELETE("DELETE"),
    COMMIT("COMMIT"),
    ABORT("ABORT"),
    REFRESH("REFRESH"),
    CREATE_TABLE("CREATE_TABLE"),
    DROP_TABLE("DROP_TABLE"),
    ALTER_SCHEMA("ALTER_SCHEMA");

    private final String wireName;

    OpType(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    public static OpType fromWireName(String wireName) {
        for (OpType value : values()) {
            if (value.wireName.equals(wireName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown operation type: " + wireName);
    }
}
