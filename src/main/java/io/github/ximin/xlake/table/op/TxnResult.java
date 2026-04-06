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

public interface TxnResult extends OpResult {

    long transactionId();

    List<String> affectedOperations();

    long commitTimestamp();

    final class CommitResult implements TxnResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final long transactionId;
        private final long commitTs;
        private final List<String> operations;

        public CommitResult(boolean success, String message, Throwable error,
                           long timestamp, long transactionId, long commitTs, List<String> operations) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.transactionId = transactionId;
            this.commitTs = commitTs;
            this.operations = operations;
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
        public long transactionId() {
            return transactionId;
        }

        @Override
        public List<String> affectedOperations() {
            return operations;
        }

        @Override
        public long commitTimestamp() {
            return commitTs;
        }

        public static CommitResult ok(long txnId, long commitTs, List<String> ops) {
            return new CommitResult(true, "Transaction committed successfully",
                    null, System.currentTimeMillis(), txnId, commitTs, ops);
        }
    }

    final class AbortResult implements TxnResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final long transactionId;
        private final String abortReason;

        public AbortResult(boolean success, String message, Throwable error,
                          long timestamp, long transactionId, String abortReason) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.transactionId = transactionId;
            this.abortReason = abortReason;
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
        public long transactionId() {
            return transactionId;
        }

        @Override
        public List<String> affectedOperations() {
            return List.of();
        }

        @Override
        public long commitTimestamp() {
            return 0L;
        }

        public String abortReason() {
            return abortReason;
        }

        public static AbortResult ok(long txnId, String reason) {
            return new AbortResult(true, "Transaction aborted: " + reason,
                    null, System.currentTimeMillis(), txnId, reason);
        }
    }
}
