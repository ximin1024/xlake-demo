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

public final class WriteResult implements OpResult, WriteResultInfo {
    private final boolean success;
    private final String message;
    private final Throwable error;
    private final long timestamp;
    private final long count;

    public WriteResult(boolean success, String message, Throwable error, long timestamp, long count) {
        this.success = success;
        this.message = message;
        this.error = error;
        this.timestamp = timestamp;
        this.count = count;
    }

    @Override
    public boolean success() {
        return success;
    }

    @Override
    public Optional<String> message() {
        return Optional.ofNullable(message);
    }

    @Override
    public Optional<Throwable> error() {
        return Optional.ofNullable(error);
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public long count() {
        return count;
    }

    public static WriteResult ok(long count) {
        return new WriteResult(true, "success", null, System.currentTimeMillis(), count);
    }

    public static WriteResult ok(String message, long count) {
        return new WriteResult(true, message, null, System.currentTimeMillis(), count);
    }

    public static WriteResult error(String msg) {
        return new WriteResult(false, msg, null, System.currentTimeMillis(), 0);
    }

    public static WriteResult error(String msg, Throwable throwable) {
        return new WriteResult(false, msg, throwable, System.currentTimeMillis(), 0);
    }
}
