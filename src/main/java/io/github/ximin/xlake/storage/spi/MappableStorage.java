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

/**
 * 支持内存映射能力的存储扩展接口。
 *
 * <p>该接口是 {@link Storage} 的可选能力扩展，而不是所有存储实现都必须具备的基础能力。</p>
 */
public interface MappableStorage extends Storage {

    /**
     * 将指定路径的区域映射到当前进程地址空间。
     */
    MappedRegion map(StoragePath path, long offset, long length, AccessMode mode) throws IOException;
}
