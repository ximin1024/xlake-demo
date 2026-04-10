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

import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.storage.block.DataBlock;

import java.util.List;
import java.util.Optional;

public interface Scan extends Read {

    List<DataBlock> plan();

    default List<String> projections() {
        return List.of();
    }

    default DataBlock.KeyRange keyRange() {
        return null;
    }

    @Override
    default List<DataBlock> getDataBlocks() {
        return plan();
    }

    @Override
    default Expression getPushedPredicate() {
        return null;
    }

    @Override
    default boolean isPrimaryKeyLookup() {
        return false;
    }

    final class Result implements Read.Result {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final long recordCount;
        private final long bytesRead;
        private final List<?> data;
        private final boolean hasMore;

        public Result(boolean success, String message, Throwable error,
                      long timestamp, long recordCount, long bytesRead, List<?> data, boolean hasMore) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.recordCount = recordCount;
            this.bytesRead = bytesRead;
            this.data = data;
            this.hasMore = hasMore;
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
        public long recordCount() {
            return recordCount;
        }

        @Override
        public long bytesRead() {
            return bytesRead;
        }

        @Override
        public List<?> data() {
            return data;
        }

        @Override
        public Optional<Object> singleResult() {
            return Optional.empty();
        }

        @Override
        public boolean hasMore() {
            return hasMore;
        }

        public static Result ok(List<?> data, boolean hasMore) {
            return new Result(true, "Scan completed", null,
                    System.currentTimeMillis(), data.size(), 0L, data, hasMore);
        }

        public static Result ok(String message, List<?> data, boolean hasMore) {
            return new Result(true, message, null,
                    System.currentTimeMillis(), data.size(), 0L, data, hasMore);
        }

        public static Result error(String msg, Throwable throwable) {
            return new Result(false, msg, throwable, System.currentTimeMillis(), 0, 0L, List.of(), false);
        }
    }
}
