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
package io.github.ximin.xlake.backend.spark;

import com.google.gson.Gson;
import io.github.ximin.xlake.backend.StructTypeHelper;
import io.github.ximin.xlake.table.XlakeTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

@Slf4j
public class SparkTable implements Table,
        SupportsRead,
        SupportsWrite {
    private final XlakeTable XlakeTable;

    // for debug
    private Gson gson;

    public SparkTable(XlakeTable XlakeTable) {
        this.XlakeTable = XlakeTable;
        gson = new Gson();
    }

    @Override
    public String name() {
        return XlakeTable.name();
    }

    @Override
    public StructType schema() {
        return StructTypeHelper.fromXlakeSchema(XlakeTable.schema());
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return null;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
        log.info("logical info class: {}, info: {}", logicalWriteInfo.getClass().getName(), gson.toJson(logicalWriteInfo));
        return new SparkWriteBuilder();
    }
}
