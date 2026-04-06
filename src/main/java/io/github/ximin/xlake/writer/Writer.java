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

import io.github.ximin.xlake.table.op.Write;

import java.util.Collection;
import java.util.Properties;

public interface Writer<T> extends AutoCloseable {
    /**
     * 初始化
     */
    void init(Properties config);

    /**
     * 写入单条数据
     */
    Write.Result write(T obj);

    /**
     * 批量写入数据
     */
    Write.Result batchWrite(Collection<T> objs);

    /**
     * 如果有buffer优化，强制刷盘/提交
     */
    void flush();

    /**
     * 获取统计信息
     */
    long recordsWritten();

    /**
     * 获取失败统计
     */
    long recordsFailed();

    /**
     * 状态检查
     */
    boolean isClosed();
}
