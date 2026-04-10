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
package io.github.ximin.xlake.storage.table.record;

import io.github.ximin.xlake.table.record.RecordView;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public record KeyValueRecordView(byte[] key, byte[] value) implements RecordView {

    @Override
    public boolean hasField(String fieldName) {
        return switch (fieldName) {
            case "key", "value", "key_base64", "value_base64", "key_length", "value_length" -> true;
            default -> false;
        };
    }

    @Override
    public Object field(String fieldName) {
        return switch (fieldName) {
            case "key" -> new String(key, StandardCharsets.UTF_8);
            case "value" -> new String(value, StandardCharsets.UTF_8);
            case "key_base64" -> Base64.getEncoder().encodeToString(key);
            case "value_base64" -> Base64.getEncoder().encodeToString(value);
            case "key_length" -> key.length;
            case "value_length" -> value.length;
            default -> null;
        };
    }
}
