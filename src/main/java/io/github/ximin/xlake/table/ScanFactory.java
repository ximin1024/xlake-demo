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
import io.github.ximin.xlake.table.op.ReadBuilder;
import io.github.ximin.xlake.table.op.Scan;

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

    public ReadBuilder<? extends Scan, ?> createBuilder() {
        return createBuilder(null);
    }

    public ReadBuilder<? extends Scan, ?> createBuilder(XlakeTable table) {
        return switch (scanType) {
            case KV -> KvScan.builder().withTable(table);
            case BATCH -> BatchScan.builder().withTable(table);
            case INDEX_ONLY, EXPRESSION_ONLY -> throw new UnsupportedOperationException("Scan type not implemented: " + scanType);
        };
    }

    public Scan createScan() {
        return createBuilder().build();
    }

    public Scan createScan(XlakeTable table) {
        return createBuilder(table).build();
    }
}
