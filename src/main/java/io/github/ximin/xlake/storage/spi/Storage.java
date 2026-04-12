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
package io.github.ximin.xlake.storage.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * 底层存储介质抽象。
 *
 * <p>这一层只描述“如何访问物理介质”，不感知表、主键、谓词、快照等表语义。
 * 本地文件系统、mmap 文件、HDFS、对象存储都应首先在这一层被统一表达。</p>
 */
public interface Storage extends AutoCloseable {

    /**
     * 存储实现名称。
     */
    String name();

    /**
     * 当前存储实现声明的能力集合。
     */
    StorageCapabilities capabilities();

    /**
     * 打开一个只读输入句柄。
     */
    InputHandle openRead(StoragePath path) throws IOException;

    /**
     * 打开一个写入句柄。
     */
    OutputHandle openWrite(StoragePath path, WriteMode mode) throws IOException;

    /**
     * 查询路径元信息。
     */
    StorageStatus status(StoragePath path) throws IOException;

    /**
     * 路径是否存在。
     */
    boolean exists(StoragePath path) throws IOException;

    /**
     * 创建目录或等价的逻辑前缀。
     */
    void createDirectories(StoragePath path) throws IOException;

    /**
     * 列出指定前缀下的直接子路径。
     */
    List<StoragePath> list(StoragePath prefix) throws IOException;

    /**
     * 删除路径。
     */
    void delete(StoragePath path) throws IOException;

    /**
     * 删除目录或逻辑前缀下的全部对象。
     */
    void deleteRecursively(StoragePath path) throws IOException;

    /**
     * 存储路径抽象。
     *
     * <p>使用 scheme + location 描述路径，避免上层直接依赖某个具体文件系统的 Path 类型。</p>
     */
    record StoragePath(String scheme, String location) implements java.io.Serializable {
    }

    /**
     * 存储元信息快照。
     */
    record StorageStatus(
            StoragePath path,
            boolean exists,
            boolean directory,
            long length,
            long lastModifiedMillis
    ) {
    }

    /**
     * 存储能力声明。
     */
    record StorageCapabilities(
            boolean supportsSequentialRead,
            boolean supportsRandomRead,
            boolean supportsSequentialWrite,
            boolean supportsAppend,
            boolean supportsRename,
            boolean supportsMmap
    ) {
    }

    /**
     * 写入模式。
     */
    enum WriteMode {
        CREATE,
        OVERWRITE,
        APPEND
    }

    /**
     * 只读输入句柄。
     */
    interface InputHandle extends AutoCloseable {

        StoragePath path();

        long position();

        long length();

        InputStream stream() throws IOException;
    }

    /**
     * 写入输出句柄。
     */
    interface OutputHandle extends AutoCloseable {

        StoragePath path();

        long position();

        OutputStream stream() throws IOException;

        void flush() throws IOException;
    }

    /**
     * 已映射的内存区域抽象。
     */
    interface MappedRegion extends AutoCloseable {

        StoragePath path();

        long offset();

        long length();

        AccessMode mode();
    }

    /**
     * 映射访问模式。
     */
    enum AccessMode {
        READ_ONLY,
        READ_WRITE
    }
}
