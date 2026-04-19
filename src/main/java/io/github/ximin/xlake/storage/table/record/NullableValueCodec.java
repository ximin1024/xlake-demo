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

import java.util.Arrays;


public final class NullableValueCodec {
    private NullableValueCodec() {
    }

    public static byte[] encode(byte[] value) {
        if (value == null) {
            return new byte[]{0};
        }
        byte[] out = new byte[value.length + 1];
        out[0] = 1;
        System.arraycopy(value, 0, out, 1, value.length);
        return out;
    }

    public static byte[] decode(byte[] stored) {
        if (stored == null) {
            return null;
        }
        if (stored.length == 0) {
            // Legacy encoding: empty bytes meant empty value.
            return stored;
        }
        byte tag = stored[0];
        if (tag == 0 && stored.length == 1) {
            return null;
        }
        if (tag == 1) {
            return Arrays.copyOfRange(stored, 1, stored.length);
        }
        return stored;
    }
}

