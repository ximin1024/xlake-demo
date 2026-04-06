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

import java.util.Optional;

public interface OpResult {

    boolean success();

    default boolean isSuccess() {
        return success();
    }

    Optional<String> message();

    Optional<Throwable> error();

    long timestamp();

    static <T extends OpResult> T success(T result) {
        return result;
    }

    static Failure failure(String msg) {
        return new Failure(msg, null);
    }

    static Failure failure(Throwable cause) {
        return new Failure(cause.getMessage(), cause);
    }

    static Failure failure(String msg, Throwable cause) {
        return new Failure(msg, cause);
    }
}
