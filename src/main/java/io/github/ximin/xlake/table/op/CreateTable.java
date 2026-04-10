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

import io.github.ximin.xlake.metastore.Metastore;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.table.TableMeta;
import io.github.ximin.xlake.table.ProtoTableMetaConverter;

import java.util.function.Supplier;

public class CreateTable implements Op<DdlResult> {
    private final static OpType TYPE = OpType.CREATE_TABLE;
    private final XlakeTable table;
    private final Supplier<Metastore> metastoreSupplier;
    private final TableMeta tableMeta;

    private CreateTable(XlakeTable table, Supplier<Metastore> metastoreSupplier, TableMeta tableMeta) {
        this.table = table;
        this.metastoreSupplier = metastoreSupplier;
        this.tableMeta = tableMeta;
    }

    @Override
    public DdlResult exec() {
        try {
            Metastore metastore = metastoreSupplier.get();
            var pbMeta = ProtoTableMetaConverter.toPb(tableMeta);
            metastore.createTable(pbMeta);
            return DdlResult.CreateResult.ok(table.name());
        } catch (Exception e) {
            return new DdlResult.CreateResult(false, "Create table failed: " + e.getMessage(),
                    e, System.currentTimeMillis(), table.name());
        }
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    public static CreateTableBuilder builder() {
        return new CreateTableBuilder();
    }

    public static class CreateTableBuilder implements TableOpBuilder<DdlResult, CreateTable> {
        private XlakeTable xlakeTable;
        private Supplier<Metastore> metastoreSupplier;
        private TableMeta tableMeta;

        public CreateTableBuilder withTable(XlakeTable xlakeTable) {
            this.xlakeTable = xlakeTable;
            return this;
        }

        public CreateTableBuilder table(XlakeTable xlakeTable) {
            return withTable(xlakeTable);
        }

        public CreateTableBuilder metastoreSupplier(Supplier<Metastore> metastoreSupplier) {
            this.metastoreSupplier = metastoreSupplier;
            return this;
        }

        public CreateTableBuilder tableMeta(TableMeta tableMeta) {
            this.tableMeta = tableMeta;
            return this;
        }

        @Override
        public XlakeTable table() {
            return xlakeTable;
        }

        @Override
        public CreateTable build() {
            if (xlakeTable == null) {
                throw new IllegalStateException("table must be set");
            }
            if (tableMeta == null) {
                throw new IllegalStateException("tableMeta must be set");
            }
            if (metastoreSupplier == null) {
                throw new IllegalStateException("metastoreSupplier must be set");
            }
            return new CreateTable(xlakeTable, metastoreSupplier, tableMeta);
        }
    }
}
