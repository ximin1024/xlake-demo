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
import io.github.ximin.xlake.table.record.RecordView;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface Find extends Read {

    Expression filter();

    List<String> projection();

    long snapshotId();

    Map<String, Comparable> primaryKeyValues();

    boolean caseSensitive();

    long limit();

    default ReadContext asContext() {
        return new DefaultReadContext(filter(), snapshotId(), caseSensitive(), limit());
    }

    interface ReadContext {
        Expression filter();
        long snapshotId();
        boolean caseSensitive();
        long limit();
    }

    record DefaultReadContext(
            Expression filter,
            long snapshotId,
            boolean caseSensitive,
            long limit
    ) implements ReadContext {
    }

    record FindResult(
            boolean found,
            RecordView data
    ) implements Serializable {
    }

    final class Result implements Read.Result {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final RecordView foundData;

        public Result(boolean success, String message, Throwable error,
                      long timestamp, RecordView foundData) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.foundData = foundData;
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
            return foundData != null ? 1 : 0;
        }

        @Override
        public long bytesRead() {
            return 0L;
        }

        @Override
        public List<?> data() {
            return foundData != null ? List.of(foundData) : List.of();
        }

        @Override
        public Optional<Object> singleResult() {
            return Optional.ofNullable(foundData);
        }

        public Optional<RecordView> foundRecord() {
            return Optional.ofNullable(foundData);
        }

        @Override
        public boolean hasMore() {
            return false;
        }

        public static Result found(RecordView data) {
            return new Result(true, "Record found", null,
                    System.currentTimeMillis(), data);
        }

        public static Result notFound() {
            return new Result(true, "Record not found", null,
                    System.currentTimeMillis(), null);
        }

        public static Result error(String msg, Throwable throwable) {
            return new Result(false, msg, throwable, System.currentTimeMillis(), null);
        }
    }
}
