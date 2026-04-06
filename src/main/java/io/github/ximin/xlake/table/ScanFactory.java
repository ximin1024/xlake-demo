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

import io.github.ximin.xlake.table.op.BatchScan;
import io.github.ximin.xlake.table.op.KvScan;
import io.github.ximin.xlake.table.op.Scan;

import java.util.List;

public class ScanFactory {
    public enum ScanType {
        KV, BATCH,
        INDEX_ONLY,
        EXPRESSION_ONLY
    }

    private final ScanType scanType;

    public ScanFactory(ScanType scanType) {
        this.scanType = scanType;
    }

    public Scan createScan() {
        return switch (scanType) {
            case KV -> new KvScan(List.of(), null, -1);
            case BATCH -> new BatchScan(List.of(), null, List.of());
            case INDEX_ONLY, EXPRESSION_ONLY -> throw new UnsupportedOperationException("Scan type not implemented: " + scanType);
        };
    }
}
