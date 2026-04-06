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

import java.util.List;
import java.util.Optional;

public interface ReadResult extends OpResult {

    long recordCount();

    long bytesRead();

    List<?> data();

    Optional<Object> singleResult();

    boolean hasMore();

    final class ScanResult implements ReadResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final long recordCount;
        private final long bytesRead;
        private final List<?> data;
        private final boolean hasMore;

        public ScanResult(boolean success, String message, Throwable error,
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

        public static ScanResult ok(List<?> data, boolean hasMore) {
            return new ScanResult(true, "Scan completed", null,
                    System.currentTimeMillis(), data.size(), 0L, data, hasMore);
        }

        public static ScanResult ok(String message, List<?> data, boolean hasMore) {
            return new ScanResult(true, message, null,
                    System.currentTimeMillis(), data.size(), 0L, data, hasMore);
        }
    }

    final class FindResult implements ReadResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final Object foundData;

        public FindResult(boolean success, String message, Throwable error,
                         long timestamp, Object foundData) {
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

        @Override
        public boolean hasMore() {
            return false;
        }

        public Object foundData() {
            return foundData;
        }

        public static FindResult found(Object data) {
            return new FindResult(true, "Record found", null,
                    System.currentTimeMillis(), data);
        }

        public static FindResult notFound() {
            return new FindResult(true, "Record not found", null,
                    System.currentTimeMillis(), null);
        }
    }
}
