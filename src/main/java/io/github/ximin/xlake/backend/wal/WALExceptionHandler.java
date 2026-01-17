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
package io.github.ximin.xlake.backend.wal;

import com.lmax.disruptor.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WALExceptionHandler<T> implements ExceptionHandler<T> {

    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        log.error("Exception processing event at sequence: {}", sequence, ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.error("Exception during WAL startup", ex);
        throw new RuntimeException("WAL startup failed", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.error("Exception during WAL shutdown", ex);
    }
}
