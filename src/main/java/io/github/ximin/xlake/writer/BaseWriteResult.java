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
package io.github.ximin.xlake.writer;

public class BaseWriteResult implements WriteResult {

    protected final boolean success;
    protected final String message;
    protected final long count;

    public BaseWriteResult(boolean success, String message, long count) {
        this.success = success;
        this.message = message;
        this.count = count;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public long getCount() {
        return count;
    }

    public static BaseWriteResult ok(long count) {
        return new BaseWriteResult(true, "Success", count);
    }

    public static BaseWriteResult error(String msg) {
        return new BaseWriteResult(false, msg, 0);
    }
}
