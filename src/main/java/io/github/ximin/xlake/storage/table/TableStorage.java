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
package io.github.ximin.xlake.storage.table;

import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.spi.Storage;
import io.github.ximin.xlake.table.TableId;
import io.github.ximin.xlake.table.op.Read;
import io.github.ximin.xlake.table.op.Write;
import java.io.IOException;
import java.util.List;

/**
 * 表数据访问抽象。
 *
 * <p>这一层在底层 {@link Storage} 之上工作，负责面向表语义提供点查、扫描、写入、Flush 等能力。
 * DynamicMmapStore、TableStore 一类运行时组件属于这一层或这一层的管理器，而不是底层介质层。</p>
 */
public interface TableStorage extends AutoCloseable {

    /**
     * 当前实例服务的表标识。
     */
    TableId tableId();

    /**
     * 执行一次写入动作。
     */
    Write.Result write(Write write) throws IOException;

    /**
     * 执行一次读取动作（Scan / Find 等）。
     */
    Read.Result read(Read read) throws IOException;

    /**
     * 返回当前可见的数据块视图。
     */
    List<DataBlock> currentDataBlocks();

    /**
     * 触发一次显式 Flush。
     */
    void flush() throws IOException;
}
