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
package io.github.ximin.xlake.table;

import io.github.ximin.xlake.table.schema.Schema;

// 表的静态信息在对象初始化的时候渲染出来，在TableOp中提供和meta层的修改交互。
public class DefaultTable implements XlakeTable {

    @Override
    public String name() {
        return "";
    }

    @Override
    public Schema schema() {
        return null;
    }

    @Override
    public Snapshot currentSnapshot() {
        return null;
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return null;
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        return null;
    }

    @Override
    public String uniqId() {
        return "";
    }
}
