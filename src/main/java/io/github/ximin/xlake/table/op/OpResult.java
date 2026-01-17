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

public sealed interface OpResult permits
        OpResult.Failure,
        OpResult.Success {

    boolean isSuccess();

    Optional<String> getMessage();

    Optional<Long> getCommitId();

    <T> Optional<T> getData(Class<T> dataType);

    record Success(String message, Long commitId, Object data) implements OpResult {
        public Success {
            commitId = commitId != null ? commitId : -1L;
            message = message != null ? message : "Operation completed successfully";
        }

        public Success(String message) {
            this(message, null, null);
        }

        public Success() {
            this(null, null, null);
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public Optional<String> getMessage() {
            return Optional.ofNullable(message);
        }

        @Override
        public Optional<Long> getCommitId() {
            return Optional.ofNullable(commitId);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getData(Class<T> dataType) {
            if (data == null) {
                return Optional.empty();
            }
            if (dataType.isInstance(data)) {
                return Optional.of((T) data);
            }
            return Optional.empty();
        }
    }

    record Failure(String message, String errorCode, Throwable cause, Long commitId) implements OpResult {
        public Failure {
            if (message == null && cause != null) {
                message = cause.getMessage();
            }
            if (message == null) {
                message = "Operation failed";
            }
        }

        public Failure(String message) {
            this(message, null, null, null);
        }

        public Failure(String message, Throwable cause) {
            this(message, null, cause, null);
        }

        public Failure(String message, String errorCode) {
            this(message, errorCode, null, null);
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public Optional<String> getMessage() {
            return Optional.of(message);
        }

        @Override
        public Optional<Long> getCommitId() {
            return Optional.ofNullable(commitId);
        }

        @Override
        public <T> Optional<T> getData(Class<T> dataType) {
            return Optional.empty();
        }

        public Optional<String> getErrorCode() {
            return Optional.ofNullable(errorCode);
        }

        public Optional<Throwable> getCause() {
            return Optional.ofNullable(cause);
        }
    }

    static OpResult success() {
        return new Success();
    }

    static OpResult success(String message) {
        return new Success(message);
    }

    static OpResult success(String message, Long commitId) {
        return new Success(message, commitId, null);
    }

    static <T> OpResult success(String message, Long commitId, T data) {
        return new Success(message, commitId, data);
    }

    static OpResult failure(String message) {
        return new Failure(message);
    }

    static OpResult failure(String message, Throwable cause) {
        return new Failure(message, cause);
    }

    static OpResult failure(String message, String errorCode) {
        return new Failure(message, errorCode);
    }
}
