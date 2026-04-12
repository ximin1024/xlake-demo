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

import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.table.read.TableReader;
import io.github.ximin.xlake.table.XlakeTable;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

@Slf4j
public class BatchScan extends BaseRead implements Scan {
    private final List<String> projections;


    public BatchScan(XlakeTable table,
                     List<DataBlock> dataBlocks,
                     Expression pushedPredicate,
                     List<String> projections,
                     TableReader reader) {
        super(table, dataBlocks, pushedPredicate, false, reader);
        this.projections = projections != null ? List.copyOf(projections) : List.of();
    }


    @Override
    public List<DataBlock> plan() {
        return getDataBlocks();
    }


    @Override
    public Expression getPushedPredicate() {
        return pushedPredicate;
    }


    @Override
    public Scan.Result exec() {
        ensureOpen();

        if (dataBlocks == null || dataBlocks.isEmpty()) {
            log.debug("BatchScan executed with empty DataBlocks for table: {}",
                    table.getTableIdentifier());
            return Scan.Result.ok(List.of(), false);
        }

        try {
            log.debug("Executing BatchScan for table: {}, blocks: {}, projections: {}",
                    table.getTableIdentifier(), dataBlocks.size(), projections);

            Scan.Result result = reader.scan(this);

            if (result.success()) {
                log.info("BatchScan completed successfully for table: {}, records: {}",
                        table.getTableIdentifier(), result.recordCount());
            } else {
                log.error("BatchScan failed for table: {}, error: {}",
                        table.getTableIdentifier(),
                        result.message().orElse("Unknown error"));
            }

            return result;

        } catch (IOException e) {
            log.error("IO error during BatchScan for table: {}",
                    table.getTableIdentifier(), e);
            return Scan.Result.error("IO error during scan: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error during BatchScan for table: {}",
                    table.getTableIdentifier(), e);
            return Scan.Result.error("Unexpected scan error: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return OpType.BATCH_SCAN;
    }

    @Override
    public List<String> projections() {
        return projections;
    }


    public static Builder builder() {
        return new Builder();
    }


    public static class Builder implements ReadBuilder<BatchScan, Builder> {
        private XlakeTable table;
        private List<DataBlock> dataBlocks;
        private Expression pushedPredicate;
        private List<String> projections;
        private TableReader reader;

        @Override
        public Builder withTable(XlakeTable table) {
            this.table = table;
            return this;
        }

        @Override
        public Builder withDataBlocks(List<DataBlock> dataBlocks) {
            this.dataBlocks = dataBlocks;
            return this;
        }

        @Override
        public Builder withPredicate(Expression predicate) {
            this.pushedPredicate = predicate;
            return this;
        }

        @Override
        public XlakeTable table() {
            return table;
        }


        public Builder withProjections(List<String> projections) {
            this.projections = projections;
            return this;
        }


        public Builder withReader(TableReader reader) {
            this.reader = Objects.requireNonNull(reader, "reader cannot be null");
            return this;
        }


        public BatchScan build() {
            Objects.requireNonNull(table, "table is required");
            Objects.requireNonNull(reader, "reader is required - use withReader() to inject TableReader");

            log.debug("Building BatchScan: table={}, blocks={}, hasReader={}",
                    table.getTableIdentifier(),
                    dataBlocks != null ? dataBlocks.size() : "null",
                    true);

            return new BatchScan(table, dataBlocks, pushedPredicate, projections, reader);
        }
    }
}
